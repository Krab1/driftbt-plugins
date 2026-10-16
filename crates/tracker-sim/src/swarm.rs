//! The loopback swarm (Phase 2): the simulated peers of one torrent talk to each other over real TCP on 127.0.0.1,
//! with the BitTorrent peer wire protocol, so their counters are bytes that really crossed a socket.
//!
//! Peer 0 starts with every piece, the others with none; each peer listens and each later peer connects to all the
//! earlier ones, so a leecher can also fetch from another leecher. The content is synthetic: every byte is a function
//! of (torrent, piece, offset), so no file is read, a received piece is checked against the hash of what the
//! generator makes for it, and a corrupt piece is thrown away and not counted.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Notify};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::identity::Personality;
use crate::pieces::{self, Bits, Strategy};
use crate::rate::RateEngine;
use crate::rng::Rng;
use crate::settings::RateCfg;
use crate::wire::{self, Msg, BLOCK};

/// Requests in flight per connection.
const PIPELINE: usize = 16;
/// Unchoke slots, besides the one optimistic unchoke.
const SLOTS: usize = 4;
const OPTIMISTIC_SECS: u64 = 30;
/// Requests being served at once per connection; more are ignored (the peer asks again).
const MAX_SERVING: usize = 64;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The synthetic data of one torrent.
pub struct Content {
    seed: u64,
    total: u64,
    piece_len: u64,
}

impl Content {
    pub fn new(seed: u64, total: u64, piece_len: u64) -> Self {
        Self { seed, total, piece_len }
    }

    pub fn pieces(&self) -> usize {
        self.total.div_ceil(self.piece_len) as usize
    }

    pub fn size(&self, piece: usize) -> u32 {
        self.piece_len.min(self.total - piece as u64 * self.piece_len) as u32
    }

    /// Bytes `begin..begin+out.len()` of `piece`.
    pub fn fill(&self, piece: usize, begin: u32, out: &mut [u8]) {
        let base = self.seed ^ (piece as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for (j, b) in out.iter_mut().enumerate() {
            let pos = u64::from(begin) + j as u64;
            let word = Rng::new(base ^ (pos / 8).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)).next_u64();
            *b = (word >> ((pos % 8) * 8)) as u8;
        }
    }

    /// What a correct piece must hash to. (SHA-256: this is our own metainfo, not a `.torrent` file.)
    pub fn digest(&self, piece: usize) -> [u8; 32] {
        let (mut h, mut buf) = (Sha256::new(), vec![0u8; BLOCK as usize]);
        let size = self.size(piece);
        let mut at = 0;
        while at < size {
            let n = (size - at).min(BLOCK) as usize;
            self.fill(piece, at, &mut buf[..n]);
            h.update(&buf[..n]);
            at += n as u32;
        }
        h.finalize().into()
    }
}

/// A byte allowance fed by a rate engine: `take` waits until the peer may move that many bytes. While the node is
/// not active (before its first announce) nothing moves.
struct Limiter {
    inner: tokio::sync::Mutex<LimiterState>,
    cap: f64,
    active: Arc<AtomicBool>,
}

struct LimiterState {
    engine: RateEngine,
    avail: f64,
    last: Instant,
}

impl Limiter {
    fn new(cfg: RateCfg, seed: u64, p: &Personality, active: Arc<AtomicBool>) -> Self {
        Self { inner: tokio::sync::Mutex::new(LimiterState { engine: RateEngine::new(cfg, seed, p), avail: 0.0, last: Instant::now() }), cap: (cfg.max_bps * 2.0).max(f64::from(2 * BLOCK)), active }
    }

    async fn set_activity(&self, f: f64) {
        self.inner.lock().await.engine.set_activity(f);
    }

    async fn take(&self, n: u32) {
        loop {
            {
                let mut s = self.inner.lock().await;
                let dt = s.last.elapsed().as_secs_f64();
                s.last = Instant::now();
                if self.active.load(Relaxed) {
                    let got = s.engine.tick(dt.min(0.5));
                    s.avail = (s.avail + got as f64).min(self.cap);
                    if s.avail >= f64::from(n) {
                        s.avail -= f64::from(n);
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

pub struct NodeCfg {
    pub up: RateCfg,
    pub down: RateCfg,
    pub personality: Personality,
    pub seed: u64,
    pub peer_id: [u8; 20],
    pub unchoke_secs: u64,
    /// Bytes this peer already has at the start (a restarted leecher): that many whole pieces from the front.
    pub initial_have: u64,
}

/// What the peer has really moved so far.
#[derive(Debug, Clone, Copy, Default)]
pub struct Snapshot {
    pub uploaded: u64,
    pub downloaded: u64,
    pub have_bytes: u64,
}

/// A connection as the choker sees it.
struct Handle {
    id: u64,
    tx: mpsc::UnboundedSender<Msg>,
    peer_interested: AtomicBool,
    am_choking: AtomicBool,
    from: AtomicU64,
    to: AtomicU64,
    cancelled: Mutex<HashSet<(u32, u32)>>,
}

pub struct Node {
    pub index: usize,
    peer_id: [u8; 20],
    info_hash: [u8; 20],
    content: Arc<Content>,
    strategy: Strategy,
    have: Mutex<Bits>,
    avail: Mutex<Vec<u16>>,
    requested: Mutex<HashMap<usize, u32>>,
    sessions: Mutex<Vec<Arc<Handle>>>,
    next_session: AtomicU64,
    up: Limiter,
    down: Limiter,
    active: Arc<AtomicBool>,
    uploaded: AtomicU64,
    downloaded: AtomicU64,
    have_bytes: AtomicU64,
    have_tx: broadcast::Sender<u32>,
    choke_now: Notify,
    rng: Mutex<Rng>,
    unchoke_secs: u64,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Test hook: serve damaged data so a leecher's hash check has something to refuse.
    pub corrupt_serving: AtomicBool,
    stopped: AtomicBool,
}

impl Node {
    pub async fn set_activity(&self, f: f64) {
        self.up.set_activity(f).await;
        self.down.set_activity(f).await;
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot { uploaded: self.uploaded.load(Relaxed), downloaded: self.downloaded.load(Relaxed), have_bytes: self.have_bytes.load(Relaxed) }
    }

    /// Lets the node move data; called after the peer's first successful announce.
    pub fn activate(&self) {
        self.active.store(true, Relaxed);
    }

    fn spawn(self: &Arc<Self>, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        let mut tasks = lock(&self.tasks);
        if self.stopped.load(Relaxed) {
            return;
        }
        tasks.retain(|t| !t.is_finished());
        tasks.push(tokio::spawn(fut));
    }

    fn start_session(self: &Arc<Self>, stream: TcpStream) {
        let node = self.clone();
        self.spawn(async move {
            if let Err(e) = run_session(node.clone(), stream).await {
                debug!("loopback peer {}: connection ended: {e}", node.index + 1);
            }
        });
    }

    /// Re-decides who is unchoked: the interested peers that moved the most data with us, plus one optimistic.
    fn rechoke(&self, last: &mut HashMap<u64, u64>, optimistic: &mut Option<u64>, rotate: bool) {
        let sessions = lock(&self.sessions).clone();
        let cands: Vec<(u64, u64)> = sessions
            .iter()
            .filter(|h| h.peer_interested.load(Relaxed))
            .map(|h| {
                let total = h.from.load(Relaxed) + h.to.load(Relaxed);
                (h.id, total - last.insert(h.id, total).unwrap_or(0))
            })
            .collect();
        last.retain(|id, _| sessions.iter().any(|h| h.id == *id));
        if rotate || optimistic.is_none_or(|o| !cands.iter().any(|c| c.0 == o)) {
            let top = select_unchoked(&cands, SLOTS, None);
            let rest: Vec<u64> = cands.iter().map(|c| c.0).filter(|id| !top.contains(id)).collect();
            *optimistic = (!rest.is_empty()).then(|| rest[lock(&self.rng).below(rest.len() as u64) as usize]);
        }
        let open = select_unchoked(&cands, SLOTS, *optimistic);
        for h in sessions {
            let want_unchoked = open.contains(&h.id);
            if want_unchoked == h.am_choking.load(Relaxed) {
                h.am_choking.store(!want_unchoked, Relaxed);
                let _ = h.tx.send(if want_unchoked { Msg::Unchoke } else { Msg::Choke });
            }
        }
    }

    async fn choker(self: Arc<Self>) {
        let (mut last, mut optimistic, mut rounds) = (HashMap::new(), None, 0u64);
        tokio::time::sleep(Duration::from_secs(1)).await;
        loop {
            self.rechoke(&mut last, &mut optimistic, rounds % (OPTIMISTIC_SECS / self.unchoke_secs.max(1)).max(1) == 0);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(self.unchoke_secs)) => rounds += 1,
                () = self.choke_now.notified() => {}
            }
        }
    }
}

/// The ids to unchoke: the `slots` interested peers with the highest rate, and the optimistic one.
pub fn select_unchoked(cands: &[(u64, u64)], slots: usize, optimistic: Option<u64>) -> HashSet<u64> {
    let mut sorted = cands.to_vec();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut open: HashSet<u64> = sorted.iter().take(slots).map(|c| c.0).collect();
    open.extend(optimistic.filter(|o| cands.iter().any(|c| c.0 == *o)));
    open
}

struct AbortOnDrop(Vec<JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.iter().for_each(JoinHandle::abort);
    }
}

struct Partial {
    buf: Vec<u8>,
    got: u32,
}

struct Session {
    node: Arc<Node>,
    h: Arc<Handle>,
    remote: Bits,
    peer_choking: bool,
    am_interested: bool,
    pending: HashMap<(u32, u32), u32>,
    partial: HashMap<u32, Partial>,
    /// The piece being requested block by block, and the next offset.
    current: Option<(u32, u32)>,
    serving: JoinSet<()>,
    seen_message: bool,
}

impl Session {
    fn send(&self, m: Msg) {
        let _ = self.h.tx.send(m);
    }

    fn release(&self, piece: u32) {
        let mut r = lock(&self.node.requested);
        if let Some(c) = r.get_mut(&(piece as usize)) {
            *c -= 1;
            if *c == 0 {
                r.remove(&(piece as usize));
            }
        }
    }

    fn drop_piece(&mut self, piece: u32, cancel: bool) {
        if self.partial.remove(&piece).is_none() {
            return;
        }
        self.release(piece);
        let blocks: Vec<(u32, u32)> = self.pending.keys().filter(|k| k.0 == piece).copied().collect();
        for k in blocks {
            if let Some(len) = self.pending.remove(&k) {
                if cancel {
                    self.send(Msg::Cancel { piece, begin: k.1, len });
                }
            }
        }
        if self.current.is_some_and(|c| c.0 == piece) {
            self.current = None;
        }
    }

    fn abandon_all(&mut self) {
        let pieces: Vec<u32> = self.partial.keys().copied().collect();
        pieces.into_iter().for_each(|p| self.drop_piece(p, false));
        self.pending.clear();
        self.current = None;
    }

    fn refresh_interest(&mut self) {
        let want = lock(&self.node.have).lacks_any_of(&self.remote);
        if want != self.am_interested {
            self.am_interested = want;
            self.send(if want { Msg::Interested } else { Msg::NotInterested });
        }
    }

    fn fill(&mut self) {
        if self.peer_choking || !self.am_interested {
            return;
        }
        while self.pending.len() < PIPELINE {
            if self.current.is_none() {
                let pick = {
                    let (have, avail, req) = (lock(&self.node.have), lock(&self.node.avail), lock(&self.node.requested));
                    pieces::pick(self.node.strategy, &have, &self.remote, &|i| req.get(&i).is_some_and(|c| *c > 0), &avail, &mut lock(&self.node.rng))
                };
                let Some(p) = pick.map(|p| p as u32).filter(|p| !self.partial.contains_key(p)) else { break };
                *lock(&self.node.requested).entry(p as usize).or_insert(0) += 1;
                self.partial.insert(p, Partial { buf: vec![0; self.node.content.size(p as usize) as usize], got: 0 });
                self.current = Some((p, 0));
            }
            let Some((piece, begin)) = self.current else { break };
            let size = self.node.content.size(piece as usize);
            let len = BLOCK.min(size - begin);
            self.send(Msg::Request { piece, begin, len });
            self.pending.insert((piece, begin), len);
            self.current = (begin + len < size).then_some((piece, begin + len));
        }
    }

    async fn on_msg(&mut self, m: Msg) -> Result<(), String> {
        let n = self.node.content.pieces();
        let seen = std::mem::replace(&mut self.seen_message, true);
        match m {
            Msg::KeepAlive => {}
            Msg::Choke => {
                self.peer_choking = true;
                self.abandon_all();
            }
            Msg::Unchoke => self.peer_choking = false,
            Msg::Interested => {
                self.h.peer_interested.store(true, Relaxed);
                self.node.choke_now.notify_one();
            }
            Msg::NotInterested => self.h.peer_interested.store(false, Relaxed),
            Msg::Have(p) => {
                let p = p as usize;
                if p >= n {
                    return Err("have for a piece that doesn't exist".into());
                }
                if !self.remote.get(p) {
                    self.remote.set(p);
                    let mut avail = lock(&self.node.avail);
                    avail[p] = avail[p].saturating_add(1);
                }
            }
            Msg::Bitfield(b) => {
                if seen {
                    return Err("bitfield after other messages".into());
                }
                self.remote = Bits::from_bytes(n, &b).ok_or("bad bitfield")?;
                let mut avail = lock(&self.node.avail);
                (0..n).filter(|&i| self.remote.get(i)).for_each(|i| avail[i] = avail[i].saturating_add(1));
            }
            Msg::Request { piece, begin, len } => {
                let ok = (piece as usize) < n && len > 0 && len <= 2 * BLOCK && u64::from(begin) + u64::from(len) <= u64::from(self.node.content.size(piece as usize));
                if !ok {
                    return Err("request outside the piece".into());
                }
                // A choked peer's requests are ignored, as are pieces we don't have.
                if !self.h.am_choking.load(Relaxed) && self.serving.len() < MAX_SERVING && lock(&self.node.have).get(piece as usize) {
                    let (node, h) = (self.node.clone(), self.h.clone());
                    self.serving.spawn(async move {
                        node.up.take(len).await;
                        if lock(&h.cancelled).remove(&(piece, begin)) || h.am_choking.load(Relaxed) {
                            return;
                        }
                        let mut data = vec![0u8; len as usize];
                        node.content.fill(piece as usize, begin, &mut data);
                        if node.corrupt_serving.load(Relaxed) {
                            data[0] ^= 0xff;
                        }
                        node.uploaded.fetch_add(u64::from(len), Relaxed);
                        h.to.fetch_add(u64::from(len), Relaxed);
                        let _ = h.tx.send(Msg::Piece { piece, begin, data });
                    });
                }
            }
            Msg::Cancel { piece, begin, .. } => {
                let mut c = lock(&self.h.cancelled);
                if c.len() >= MAX_SERVING * 4 {
                    c.clear();
                }
                c.insert((piece, begin));
            }
            Msg::Piece { piece, begin, data } => {
                match self.pending.get(&(piece, begin)) {
                    None => return Ok(()), // cancelled or never asked for
                    Some(len) if *len != data.len() as u32 => return Err("block of the wrong length".into()),
                    Some(_) => {
                        self.pending.remove(&(piece, begin));
                    }
                }
                self.node.down.take(data.len() as u32).await;
                self.h.from.fetch_add(data.len() as u64, Relaxed);
                let Some(part) = self.partial.get_mut(&piece) else { return Ok(()) };
                part.buf[begin as usize..begin as usize + data.len()].copy_from_slice(&data);
                part.got += data.len() as u32;
                if part.got as usize == part.buf.len() {
                    self.finish_piece(piece);
                }
            }
        }
        Ok(())
    }

    fn finish_piece(&mut self, piece: u32) {
        let Some(part) = self.partial.remove(&piece) else { return };
        self.release(piece);
        if self.current.is_some_and(|c| c.0 == piece) {
            self.current = None;
        }
        let good = Sha256::digest(&part.buf).as_slice() == self.node.content.digest(piece as usize);
        if !good {
            warn!("loopback peer {}: piece {piece} failed its hash check; discarded", self.node.index + 1);
            return;
        }
        let new = {
            let mut have = lock(&self.node.have);
            let new = !have.get(piece as usize);
            have.set(piece as usize);
            new
        };
        if new {
            let size = part.buf.len() as u64;
            self.node.have_bytes.fetch_add(size, Relaxed);
            self.node.downloaded.fetch_add(size, Relaxed);
            let _ = self.node.have_tx.send(piece);
        }
    }

    fn on_have_event(&mut self, piece: u32) {
        if !self.remote.get(piece as usize) {
            self.send(Msg::Have(piece));
        }
        // Another connection finished a piece this one was still fetching (endgame): cancel the duplicate.
        self.drop_piece(piece, true);
    }

    fn cleanup(&mut self) {
        self.abandon_all();
        let mut avail = lock(&self.node.avail);
        (0..self.node.content.pieces()).filter(|&i| self.remote.get(i)).for_each(|i| avail[i] = avail[i].saturating_sub(1));
        drop(avail);
        lock(&self.node.sessions).retain(|h| h.id != self.h.id);
    }
}

async fn run_session(node: Arc<Node>, stream: TcpStream) -> Result<(), String> {
    let es = |e: std::io::Error| e.to_string();
    let (mut rd, mut wr) = stream.into_split();
    wr.write_all(&wire::handshake(&node.info_hash, &node.peer_id)).await.map_err(es)?;
    let mut hs = [0u8; 68];
    tokio::time::timeout(Duration::from_secs(10), rd.read_exact(&mut hs)).await.map_err(|_| "handshake timed out")?.map_err(es)?;
    let (ih, pid) = wire::parse_handshake(&hs)?;
    if ih != node.info_hash || pid == node.peer_id {
        return Err("wrong torrent or connected to itself".into());
    }
    let (tx, mut out) = mpsc::unbounded_channel::<Msg>();
    let writer = tokio::spawn(async move {
        let mut keepalive = tokio::time::interval_at(Instant::now() + Duration::from_secs(120), Duration::from_secs(120));
        loop {
            let bytes = tokio::select! {
                m = out.recv() => match m { Some(m) => m.encode(), None => break },
                _ = keepalive.tick() => Msg::KeepAlive.encode(),
            };
            if wr.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });
    let (frame_tx, mut frames) = mpsc::unbounded_channel();
    let reader = tokio::spawn(async move {
        loop {
            let m = wire::read_msg(&mut rd).await;
            let end = m.is_err();
            if frame_tx.send(m).is_err() || end {
                break;
            }
        }
    });
    let _tasks = AbortOnDrop(vec![writer, reader]);

    let n = node.content.pieces();
    let h = Arc::new(Handle { id: node.next_session.fetch_add(1, Relaxed), tx, peer_interested: AtomicBool::new(false), am_choking: AtomicBool::new(true), from: AtomicU64::new(0), to: AtomicU64::new(0), cancelled: Mutex::new(HashSet::new()) });
    let mut events = node.have_tx.subscribe();
    lock(&node.sessions).push(h.clone());
    let mut s = Session { node: node.clone(), h, remote: Bits::new(n), peer_choking: true, am_interested: false, pending: HashMap::new(), partial: HashMap::new(), current: None, serving: JoinSet::new(), seen_message: false };
    {
        let have = lock(&node.have);
        if have.count() > 0 {
            s.send(Msg::Bitfield(have.as_bytes().to_vec()));
        }
    }
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let result = loop {
        tokio::select! {
            f = frames.recv() => match f {
                None => break Ok(()),
                Some(Err(e)) => break Err(e),
                Some(Ok(m)) => if let Err(e) = s.on_msg(m).await { break Err(e) },
            },
            ev = events.recv() => if let Ok(p) = ev { s.on_have_event(p) },
            _ = tick.tick() => {}
        }
        while s.serving.try_join_next().is_some() {}
        s.refresh_interest();
        s.fill();
    };
    s.cleanup();
    result
}

/// All the nodes of one torrent's loopback swarm. Dropping it stops every task.
pub struct Swarm {
    pub nodes: Vec<Arc<Node>>,
}

impl Drop for Swarm {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Swarm {
    pub async fn start(info_hash: [u8; 20], content: Arc<Content>, strategy: Strategy, cfgs: Vec<NodeCfg>) -> Result<Self, String> {
        let n = content.pieces();
        let mut nodes = Vec::new();
        let mut listeners = Vec::new();
        for (index, cfg) in cfgs.into_iter().enumerate() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.map_err(|e| e.to_string())?;
            let active = Arc::new(AtomicBool::new(false));
            let seed = index == 0;
            let mut have = if seed { Bits::full(n) } else { Bits::new(n) };
            let mut have_bytes = if seed { content.total } else { 0 };
            for i in 0..n {
                if !seed && have_bytes + u64::from(content.size(i)) <= cfg.initial_have {
                    have.set(i);
                    have_bytes += u64::from(content.size(i));
                }
            }
            nodes.push(Arc::new(Node {
                index,
                peer_id: cfg.peer_id,
                info_hash,
                content: content.clone(),
                strategy,
                have: Mutex::new(have),
                avail: Mutex::new(vec![0; n]),
                requested: Mutex::new(HashMap::new()),
                sessions: Mutex::new(Vec::new()),
                next_session: AtomicU64::new(0),
                up: Limiter::new(cfg.up, cfg.seed ^ 3, &cfg.personality, active.clone()),
                down: Limiter::new(cfg.down, cfg.seed ^ 4, &cfg.personality, active.clone()),
                active,
                uploaded: AtomicU64::new(0),
                downloaded: AtomicU64::new(0),
                have_bytes: AtomicU64::new(have_bytes),
                have_tx: broadcast::channel(1024).0,
                choke_now: Notify::new(),
                rng: Mutex::new(Rng::new(cfg.seed ^ 5)),
                unchoke_secs: cfg.unchoke_secs,
                tasks: Mutex::new(Vec::new()),
                corrupt_serving: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
            }));
            listeners.push(listener);
        }
        let addrs: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().map_err(|e| e.to_string())).collect::<Result<_, _>>()?;
        let swarm = Self { nodes };
        let nodes = &swarm.nodes;
        for (node, listener) in nodes.iter().zip(listeners) {
            let acceptor = node.clone();
            node.spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    acceptor.start_session(stream);
                }
            });
            node.spawn(node.clone().choker());
        }
        for i in 1..nodes.len() {
            for addr in &addrs[..i] {
                let stream = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
                nodes[i].start_session(stream);
            }
        }
        Ok(swarm)
    }

    pub fn stop(&self) {
        for n in &self.nodes {
            let mut tasks = lock(&n.tasks);
            n.stopped.store(true, Relaxed);
            tasks.drain(..).for_each(|t| t.abort());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::RateProfile;

    fn cfg(i: usize) -> NodeCfg {
        let fast = RateCfg { profile: RateProfile::ConstantJitter, min_bps: 40e6, max_bps: 50e6 };
        NodeCfg { up: fast, down: fast, personality: Personality { rate_scale: 1.0, idle_prob: 0.0, burst_prob: 0.0, interval_factor: 1.0 }, seed: i as u64 + 1, peer_id: [i as u8 + 1; 20], unchoke_secs: 10, initial_have: 0 }
    }

    async fn swarm(n: usize, total: u64, strategy: Strategy) -> Swarm {
        let content = Arc::new(Content::new(77, total, 32 * 1024));
        let s = Swarm::start([9; 20], content, strategy, (0..n).map(cfg).collect()).await.unwrap();
        s.nodes.iter().for_each(|n| n.activate());
        s
    }

    async fn wait_until(what: &str, f: impl Fn() -> bool) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[test]
    fn content_is_deterministic_and_pieces_differ() {
        let c = Content::new(1, 100_000, 32 * 1024);
        assert_eq!((c.pieces(), c.size(0), c.size(3)), (4, 32768, 100_000 - 3 * 32768));
        let (mut a, mut b) = ([0u8; 64], [0u8; 64]);
        c.fill(0, 100, &mut a);
        c.fill(0, 100, &mut b);
        assert_eq!(a, b);
        let mut whole = vec![0u8; 32768];
        c.fill(0, 0, &mut whole);
        assert_eq!(&whole[100..164], &a, "a block is a slice of the piece");
        assert_ne!(c.digest(0), c.digest(1));
    }

    #[test]
    fn choker_picks_the_fastest_and_the_optimistic() {
        let cands = [(1, 10), (2, 50), (3, 30), (4, 20), (5, 0), (6, 5)];
        assert_eq!(select_unchoked(&cands, 2, Some(6)), HashSet::from([2, 3, 6]));
        assert_eq!(select_unchoked(&cands, 2, Some(99)), HashSet::from([2, 3]), "an optimistic peer that left is ignored");
        assert!(select_unchoked(&[], 4, None).is_empty());
    }

    #[tokio::test]
    async fn three_peers_move_real_verified_data() {
        let total = 1_000_000 + 5; // an odd tail: the last piece is short
        let s = swarm(3, total, Strategy::RarestFirst).await;
        wait_until("both leechers to finish", || s.nodes[1].snapshot().have_bytes == total && s.nodes[2].snapshot().have_bytes == total).await;
        let snaps: Vec<Snapshot> = s.nodes.iter().map(|n| n.snapshot()).collect();
        assert_eq!((snaps[0].downloaded, snaps[1].downloaded, snaps[2].downloaded), (0, total, total), "the seed downloads nothing");
        let up: u64 = snaps.iter().map(|x| x.uploaded).sum();
        assert!(up >= 2 * total, "uploaded {up} covers what was downloaded");
        assert!(snaps[0].uploaded > 0, "the seed uploaded");
        s.stop();
    }

    #[tokio::test]
    async fn a_restarted_leecher_keeps_what_it_had() {
        let content = Arc::new(Content::new(3, 640 * 1024, 32 * 1024));
        let mut cfgs: Vec<NodeCfg> = (0..2).map(cfg).collect();
        cfgs[1].initial_have = 320 * 1024 + 5; // ten whole pieces
        let s = Swarm::start([7; 20], content, Strategy::RarestFirst, cfgs).await.unwrap();
        assert_eq!(s.nodes[1].snapshot().have_bytes, 320 * 1024);
        s.nodes.iter().for_each(|n| n.activate());
        wait_until("the rest to arrive", || s.nodes[1].snapshot().have_bytes == 640 * 1024).await;
        assert_eq!(s.nodes[1].snapshot().downloaded, 320 * 1024, "only the missing half crossed the socket");
    }

    #[tokio::test]
    async fn endgame_bias_finishes_too() {
        let s = swarm(2, 300_000, Strategy::EndgameBias).await;
        wait_until("the leecher to finish", || s.nodes[1].snapshot().have_bytes == 300_000).await;
        assert_eq!(s.nodes[1].snapshot().downloaded, 300_000, "a duplicated piece is counted once");
        s.stop();
    }

    #[tokio::test]
    async fn nothing_moves_before_activation_and_corrupt_data_is_not_counted() {
        let content = Arc::new(Content::new(5, 200_000, 32 * 1024));
        let s = Swarm::start([8; 20], content, Strategy::Sequential, (0..2).map(cfg).collect()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(s.nodes[1].snapshot().downloaded, 0, "inactive nodes move nothing");
        s.nodes[0].corrupt_serving.store(true, Relaxed);
        s.nodes.iter().for_each(|n| n.activate());
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let leech = s.nodes[1].snapshot();
        assert_eq!((leech.downloaded, leech.have_bytes), (0, 0), "failed pieces count for nothing");
        assert!(s.nodes[0].snapshot().uploaded > 0, "the data was sent, and refused");
        s.stop();
    }
}
