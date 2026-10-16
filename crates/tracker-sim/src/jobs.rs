//! Per-torrent jobs. `Sim` is the one owner of the state behind the four simulator calls: the list of torrents
//! Drift told us about, the persisted selection, and one background job (a task per simulated peer) for every
//! torrent that is switched on. The RPC loop calls it and returns at once; the work happens on the runtime.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::activity;
use crate::identity::Identity;
use crate::peer::Peer;
use crate::profiles::{self, Profile};
use crate::proxy;
use crate::settings::{Mode, ProxyMode, Settings};
use crate::swarm::{Content, Node, NodeCfg, Swarm};
use crate::state::{now_iso, Store, TorrentSel};
use crate::tracker::{self, Params};

/// How often a peer advances its counters.
const TICK: Duration = Duration::from_millis(200);
/// Waiting for a torrent's size (a magnet without details) or its trackers.
const WAIT_FOR_DETAILS: Duration = Duration::from_secs(5);
const NO_TRACKER_RETRY: Duration = Duration::from_secs(10);
/// Per tracker for a normal announce (the client allows the same), and for the goodbye.
const ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(15);
const STOPPED_TRY_TIMEOUT: Duration = Duration::from_secs(2);
const STOP_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(4);

/// The parts of Drift's torrent object this plugin uses; everything else is ignored.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Torrent {
    #[serde(default)]
    pub info_hash: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub total_size: u64,
    #[serde(default)]
    pub trackers: Vec<String>,
}

pub fn valid_hash(h: &str) -> bool {
    h.len() == 40 && h.bytes().all(|b| b.is_ascii_hexdigit())
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Shared {
    settings: Settings,
    registry: Mutex<HashMap<String, Torrent>>,
    store: Mutex<Store>,
    /// Encoded `name=value` pairs added to every tracker request (API key, extra query).
    extra: String,
}

impl Shared {
    fn torrent(&self, hash: &str) -> Option<Torrent> {
        lock(&self.registry).get(hash).cloned()
    }
}

struct Job {
    stop: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
    lo: Option<Arc<LoopCtx>>,
}

/// The loopback swarm of one torrent, built by whichever of its peers gets there first (once the size is known).
struct LoopCtx {
    swarm: tokio::sync::OnceCell<Swarm>,
    ids: Vec<Identity>,
    /// Bytes each peer already had when this run began (from its saved counters).
    had: Vec<u64>,
}

impl LoopCtx {
    async fn node(&self, sh: &Shared, hash: &str, ih: [u8; 20], total: u64, index: usize) -> Result<Arc<Node>, String> {
        let s = &sh.settings;
        let swarm = self
            .swarm
            .get_or_try_init(|| async {
                let cfgs = self
                    .ids
                    .iter()
                    .zip(&self.had)
                    .map(|(id, had)| {
                        let mut peer_id = [b'-'; 20];
                        peer_id.iter_mut().zip(id.peer_id.bytes()).for_each(|(d, b)| *d = b);
                        NodeCfg { up: s.upload, down: s.download, personality: id.personality(s.stealth.spread()), seed: id.seed(), peer_id, unchoke_secs: profiles::resolve(&id.client_profile, "", "").unchoke_secs, initial_have: *had }
                    })
                    .collect();
                let seed = u64::from_be_bytes(ih[..8].try_into().expect("8 bytes"));
                let swarm = Swarm::start(ih, Arc::new(Content::new(seed, total, 256 * 1024)), s.piece_strategy, cfgs).await?;
                info!("loopback swarm started info_hash={hash} peers={}", self.ids.len());
                Ok::<_, String>(swarm)
            })
            .await?;
        swarm.nodes.get(index).cloned().ok_or_else(|| "no such loopback peer".to_string())
    }
}

pub struct Sim {
    shared: Arc<Shared>,
    rt: Handle,
    jobs: HashMap<String, Job>,
    /// Tasks of switched-off jobs that are still sending `stopped`, by torrent.
    stopping: HashMap<String, Vec<JoinHandle<()>>>,
}

impl Sim {
    pub fn new(settings: Settings, api_key: &str, rt: Handle) -> Self {
        let dir = settings.data_dir.as_deref().map_or_else(|| PathBuf::from("../.tracker-sim"), PathBuf::from);
        let store = Store::load(dir.join("state.json"));
        let mut extra = Vec::new();
        if !api_key.is_empty() && !settings.api_key_param.is_empty() {
            extra.push(format!("{}={}", tracker::pct(settings.api_key_param.as_bytes()), tracker::pct(api_key.as_bytes())));
        }
        if !settings.extra_query.is_empty() {
            extra.push(settings.extra_query.clone());
        }
        let enabled = store.state.torrents.values().filter(|t| t.enabled).count();
        info!("state loaded identities={} enabled_torrents={enabled}", store.state.identities.len());
        Self {
            shared: Arc::new(Shared { settings, registry: Mutex::new(HashMap::new()), store: Mutex::new(store), extra: extra.join("&") }),
            rt,
            jobs: HashMap::new(),
            stopping: HashMap::new(),
        }
    }

    /// Replaces the known list. Torrents that were switched on before a restart start now that their details
    /// are known; jobs of torrents Drift no longer has stop.
    pub fn torrents_updated(&mut self, list: Vec<Torrent>) {
        info!("torrents_updated count={}", list.len());
        let (gone, resume) = {
            let mut reg = lock(&self.shared.registry);
            reg.clear();
            for mut t in list {
                t.info_hash.make_ascii_lowercase();
                if valid_hash(&t.info_hash) {
                    reg.insert(t.info_hash.clone(), t);
                }
            }
            let store = lock(&self.shared.store);
            let gone: Vec<String> = self.jobs.keys().filter(|h| !reg.contains_key(*h)).cloned().collect();
            let resume: Vec<String> =
                store.state.torrents.iter().filter(|(h, t)| t.enabled && reg.contains_key(*h) && !self.jobs.contains_key(*h)).map(|(h, _)| h.clone()).collect();
            (gone, resume)
        };
        for h in gone {
            info!("torrent {h} is gone from Drift; stopping its simulation");
            self.stop_job(&h);
        }
        for h in resume {
            self.start_job(&h);
        }
    }

    pub fn selection_changed(&mut self, hash: &str, enabled: bool, torrent: Option<Torrent>) -> Result<(), String> {
        let hash = hash.to_ascii_lowercase();
        if !valid_hash(&hash) {
            return Err("info_hash must be 40 hex digits".into());
        }
        if let Some(mut t) = torrent {
            t.info_hash.clone_from(&hash);
            lock(&self.shared.registry).insert(hash.clone(), t);
        }
        let s = &self.shared.settings;
        {
            let mut store = lock(&self.shared.store);
            let sel = store.state.torrents.entry(hash.clone()).or_default();
            if enabled {
                if !sel.enabled || sel.mode.is_none() {
                    *sel = TorrentSel { enabled: true, mode: Some(s.default_mode), peer_count: Some(if s.default_mode == Mode::LoopbackPair { s.default_peer_count.max(2) } else { s.default_peer_count }), started_at: Some(now_iso()) };
                }
            } else {
                *sel = TorrentSel::default();
            }
            store.save();
        }
        info!("selection_changed info_hash={hash} enabled={enabled}");
        if enabled {
            self.start_job(&hash);
        } else {
            self.stop_job(&hash);
        }
        Ok(())
    }

    /// Every torrent we know of (listed by Drift or saved), with the switch and a label for the ones that are on.
    pub fn selections(&self) -> Vec<Value> {
        // registry first, as in `torrents_updated`
        let reg = lock(&self.shared.registry);
        let store = lock(&self.shared.store);
        let hashes: BTreeSet<&String> = store.state.torrents.keys().chain(reg.keys()).collect();
        hashes
            .into_iter()
            .map(|h| match store.state.torrents.get(h) {
                Some(TorrentSel { enabled: true, mode, peer_count, .. }) => {
                    let n = peer_count.unwrap_or(1);
                    let label = format!("Simulating: {}, {n} peer{}", mode.unwrap_or(Mode::SeedOnly).name(), if n == 1 { "" } else { "s" });
                    json!({"info_hash": h, "enabled": true, "label": label})
                }
                _ => json!({"info_hash": h, "enabled": false}),
            })
            .collect()
    }

    /// Stops every job (each peer sends its `stopped` event), waits a few seconds for that, and saves.
    pub fn shutdown(&mut self) {
        let mut handles = Vec::new();
        for (_, job) in self.jobs.drain() {
            let _ = job.stop.send(true);
            job.lo.iter().filter_map(|c| c.swarm.get()).for_each(Swarm::stop);
            handles.extend(job.handles);
        }
        handles.extend(self.stopping.drain().flat_map(|(_, h)| h));
        self.rt.block_on(async {
            let _ = tokio::time::timeout(STOP_ANNOUNCE_TIMEOUT + Duration::from_secs(1), async {
                for h in handles {
                    let _ = h.await;
                }
            })
            .await;
        });
        lock(&self.shared.store).save();
        info!("shutdown complete");
    }

    fn start_job(&mut self, hash: &str) {
        if self.jobs.contains_key(hash) {
            return;
        }
        // A peer that is still saying goodbye must finish first, or the new one would announce `started` before
        // the old `stopped` and resume from counters the old one hasn't saved yet.
        // (Waiting happens in the background: the host only gives `selection_changed` 3 s.)
        let (gate_tx, gate) = watch::channel(false);
        let old = self.stopping.remove(hash).unwrap_or_default();
        self.rt.spawn(async move {
            let _ = tokio::time::timeout(STOP_ANNOUNCE_TIMEOUT + Duration::from_secs(2), async {
                for h in old {
                    let _ = h.await;
                }
            })
            .await;
            let _ = gate_tx.send(true);
        });
        let (mode, mut peers) = {
            let store = lock(&self.shared.store);
            match store.state.torrents.get(hash) {
                Some(t) if t.enabled => (t.mode.unwrap_or(self.shared.settings.default_mode), t.peer_count.unwrap_or(1)),
                _ => return,
            }
        };
        if mode == Mode::LoopbackPair {
            peers = peers.max(2); // one seeds, the rest download from it and from each other
        }
        let (tx, rx) = watch::channel(false);
        let s = &self.shared.settings;
        let ids: Vec<Identity> = (0..peers).map(|i| lock(&self.shared.store).identity(i, &s.client_profile, &s.custom_prefix)).collect();
        let had: Vec<u64> = {
            let store = lock(&self.shared.store);
            ids.iter().map(|id| s.persist_counters.then(|| store.state.counters.get(&format!("{}:{hash}", id.identity_id)).and_then(|c| c.have_bytes)).flatten().unwrap_or(0)).collect()
        };
        let lo = (mode == Mode::LoopbackPair).then(|| Arc::new(LoopCtx { swarm: tokio::sync::OnceCell::new(), ids: ids.clone(), had }));
        let mut handles = Vec::new();
        let mut taken: Vec<usize> = Vec::new();
        for (i, id) in ids.into_iter().enumerate() {
            let saved_proxy = if s.proxy_mode == ProxyMode::PerPeer { id.proxy_id.as_deref() } else { None };
            let chosen = proxy::assign(&s.proxies, s.proxy_mode, saved_proxy, &taken);
            let def = match &chosen {
                Ok(p) => p.map(|n| {
                    taken.push(n);
                    &s.proxies[n]
                }),
                Err(e) => {
                    warn!("{} not started: {e}", id.identity_id);
                    continue;
                }
            };
            let client = match proxy::client(def, s.local_address) {
                Ok(c) => c,
                Err(e) => {
                    warn!("{} not started: {e}", id.identity_id);
                    continue;
                }
            };
            lock(&self.shared.store).set_proxy(i, def.filter(|_| s.proxy_mode == ProxyMode::PerPeer).map(|d| d.id.clone()));
            info!("{} uses {}", id.identity_id, def.map_or("a direct connection", |d| d.id.as_str()));
            handles.push(self.rt.spawn(run_peer(self.shared.clone(), client, hash.to_string(), id, mode, lo.clone().map(|c| (c, i)), gate.clone(), rx.clone())));
        }
        let name = self.shared.torrent(hash).map(|t| t.name).unwrap_or_default();
        info!("simulation started info_hash={hash} name={name:?} mode={} peers={peers}", mode.name());
        self.jobs.insert(hash.to_string(), Job { stop: tx, handles, lo });
    }

    /// Tells the job to stop. Its peers send `stopped` on their own; nothing waits for them here.
    fn stop_job(&mut self, hash: &str) {
        if let Some(job) = self.jobs.remove(hash) {
            let _ = job.stop.send(true);
            job.lo.iter().filter_map(|c| c.swarm.get()).for_each(Swarm::stop);
            self.stopping.retain(|_, v| {
                v.retain(|h| !h.is_finished());
                !v.is_empty()
            });
            self.stopping.insert(hash.to_string(), job.handles);
        }
    }
}

/// Sleeps; true if the job was told to stop meanwhile (or was dropped).
async fn sleep_or_stop(stop: &mut watch::Receiver<bool>, d: Duration) -> bool {
    tokio::select! {
        () = tokio::time::sleep(d) => *stop.borrow(),
        r = stop.changed() => r.is_err() || *stop.borrow(),
    }
}

/// Resolves when the job is told to stop (or dropped).
async fn until_stop(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}

enum Outcome {
    NoTracker,
    Done(Option<u64>),
    Failed(String),
}

/// Scheme and host only, for logs (the full address may hold a passkey).
fn origin(url: &str) -> &str {
    let start = url.find("://").map_or(0, |i| i + 3);
    url[start..].find('/').map_or(url, |i| &url[..start + i])
}

fn http_trackers(sh: &Shared, hash: &str) -> Vec<String> {
    sh.torrent(hash).map(|t| t.trackers).unwrap_or_default().into_iter().filter(|u| u.starts_with("http://") || u.starts_with("https://")).collect()
}

#[allow(clippy::too_many_arguments)] // one call per announce; a bundle struct would only rename the arguments
async fn announce_round(sh: &Shared, client: &reqwest::Client, hash: &str, ih: &[u8; 20], peer: &Peer, profile: &Profile, event: Option<&str>, per_tracker: Duration) -> Outcome {
    let trackers = http_trackers(sh, hash);
    if trackers.is_empty() {
        return Outcome::NoTracker;
    }
    let p = Params {
        info_hash: ih,
        peer_id: &peer.identity.peer_id,
        port: peer.identity.port,
        uploaded: peer.uploaded,
        downloaded: peer.downloaded,
        left: peer.left(),
        num_want: if event == Some("stopped") { 0 } else { profile.num_want },
        event,
        key: &peer.identity.key,
        transmission_style: profile.transmission_query,
    };
    debug!("announce identity={} uploaded={} downloaded={} left={} event={}", peer.identity.identity_id, p.uploaded, p.downloaded, p.left, event.unwrap_or("none"));
    let (mut interval, mut ok, mut last_err) = (None, false, String::new());
    for t in &trackers {
        let url = tracker::announce_url(t, &p, sh.settings.info_hash_encoding, &sh.extra);
        let answer = tokio::time::timeout(per_tracker, tracker::get(client, &url, &profile.user_agent)).await.unwrap_or_else(|_| Err("timed out".into()));
        match answer.and_then(|b| tracker::parse_answer(&b)) {
            Ok(a) => {
                ok = true;
                interval = interval.or(a.interval);
            }
            Err(e) => {
                warn!("announce failed identity={} tracker={} {e}", peer.identity.identity_id, origin(t));
                last_err = e;
            }
        }
    }
    if ok {
        Outcome::Done(interval)
    } else {
        Outcome::Failed(last_err)
    }
}

async fn scrape_round(sh: &Shared, client: &reqwest::Client, hash: &str, ih: &[u8; 20], profile: &Profile) {
    for t in http_trackers(sh, hash) {
        let Some(url) = tracker::scrape_url(&t, ih, sh.settings.info_hash_encoding, &sh.extra) else { continue };
        if let Err(e) = tracker::get(client, &url, &profile.user_agent).await {
            debug!("scrape failed tracker={} {e}", origin(&t));
        }
    }
}

fn flush(sh: &Shared, key: &str, peer: &Peer) {
    if sh.settings.persist_counters {
        let mut store = lock(&sh.store);
        store.state.counters.insert(key.to_string(), peer.counters());
        store.save();
    }
}

/// One simulated peer of one torrent, from start to stop.
#[allow(clippy::too_many_arguments)] // started from one place; a bundle struct would only rename the arguments
async fn run_peer(sh: Arc<Shared>, client: reqwest::Client, hash: String, id: Identity, mode: Mode, lo: Option<(Arc<LoopCtx>, usize)>, mut gate: watch::Receiver<bool>, mut stop: watch::Receiver<bool>) {
    let s = &sh.settings;
    // A previous run of this torrent is still saying goodbye: its final counters must be saved first.
    while !*gate.borrow() {
        tokio::select! {
            r = gate.changed() => if r.is_err() { break },
            () = until_stop(&mut stop) => return,
        }
    }
    let profile = profiles::resolve(&id.client_profile, &s.custom_prefix, &s.custom_user_agent);
    let Some(ih) = tracker::hash_bytes(&hash) else { return };
    let total = loop {
        match sh.torrent(&hash) {
            Some(t) if t.total_size > 0 => break t.total_size,
            _ => {}
        }
        if sleep_or_stop(&mut stop, WAIT_FOR_DETAILS).await {
            return;
        }
    };
    let key = format!("{}:{hash}", id.identity_id);
    let saved = if s.persist_counters { lock(&sh.store).state.counters.get(&key).cloned() } else { None };
    // In a loopback swarm the first peer seeds and the others download.
    let (peer_mode, node) = match &lo {
        Some((ctx, i)) => match ctx.node(&sh, &hash, ih, total, *i).await {
            Ok(n) => (if *i == 0 { Mode::SeedOnly } else { Mode::DownloadAndSeed }, Some(n)),
            Err(e) => {
                warn!("{} not started: loopback swarm failed: {e}", id.identity_id);
                return;
            }
        },
        None => (mode, None),
    };
    let mut peer = Peer::new(id, peer_mode, total, saved.as_ref(), s);
    if node.is_some() {
        peer.use_measured();
    }
    let first = Instant::now() + peer.initial_delay(s.start_delay_max_ms);
    let (mut next, mut next_scrape) = (first, first + peer.jittered(s.scrape_interval_ms));
    let (mut started, mut fails, mut last) = (false, 0u32, Instant::now());
    let mut next_activity = Instant::now();
    loop {
        if sleep_or_stop(&mut stop, TICK).await {
            break;
        }
        let now = Instant::now();
        // ponytail: counters pause while a request is in flight (up to its 15 s timeout); make announces a
        // separate task if that gap ever shows up on a tracker's graph.
        let dt = now.duration_since(last).as_secs_f64();
        last = now;
        if s.activity_pattern && now >= next_activity {
            let unix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
            let f = activity::factor(peer.identity.seed(), unix);
            peer.set_activity(f);
            if let Some(n) = &node {
                n.set_activity(f).await;
            }
            next_activity = now + Duration::from_secs(60);
        }
        if started {
            match &node {
                Some(n) => peer.measure(&n.snapshot()),
                None => peer.step(dt),
            }
        }
        if now >= next {
            let event = if !started {
                s.send_started.then_some("started")
            } else if peer.take_completed() {
                Some("completed")
            } else {
                None
            };
            let round = announce_round(&sh, &client, &hash, &ih, &peer, &profile, event, ANNOUNCE_TIMEOUT);
            let outcome = tokio::select! { o = round => o, () = until_stop(&mut stop) => break };
            match outcome {
                Outcome::Done(hint) => {
                    started = true;
                    if let Some(n) = &node {
                        n.activate();
                    }
                    fails = 0;
                    peer.last_announce = Some(now_iso());
                    next = Instant::now() + peer.announce_delay(hint.filter(|_| s.honor_tracker_interval).map(|i| i.clamp(10, 3600)));
                    flush(&sh, &key, &peer);
                }
                Outcome::NoTracker => {
                    if event == Some("completed") {
                        peer.rearm_completed();
                    }
                    next = Instant::now() + NO_TRACKER_RETRY;
                }
                Outcome::Failed(e) => {
                    fails += 1;
                    if event == Some("completed") {
                        peer.rearm_completed();
                    }
                    let retry = peer.retry_delay(fails);
                    warn!("tracker request failed identity={} {e} retry_ms={}", peer.identity.identity_id, retry.as_millis());
                    next = Instant::now() + retry;
                }
            }
        }
        if s.scrape_enabled && started && Instant::now() >= next_scrape {
            scrape_round(&sh, &client, &hash, &ih, &profile).await;
            next_scrape = Instant::now() + peer.jittered(s.scrape_interval_ms);
        }
    }
    if started {
        let stopped = announce_round(&sh, &client, &hash, &ih, &peer, &profile, Some("stopped"), STOPPED_TRY_TIMEOUT);
        if tokio::time::timeout(STOP_ANNOUNCE_TIMEOUT, stopped).await.is_err() {
            warn!("stopped announce timed out identity={}", peer.identity.identity_id);
        }
        peer.last_announce = Some(now_iso());
        flush(&sh, &key, &peer);
    }
    debug!("peer {key} stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent(h: &str) -> Torrent {
        Torrent { info_hash: h.into(), name: "t".into(), total_size: 0, trackers: vec![] }
    }

    fn is_on(sim: &Sim, h: &str) -> Option<bool> {
        sim.selections().iter().find(|r| r["info_hash"] == h).map(|r| r["enabled"] == true)
    }

    #[test]
    fn selection_is_per_torrent_and_survives_restart() {
        let (a, b) = ("a".repeat(40), "b".repeat(40));
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!("tracker-sim-jobs-{}", crate::rng::entropy()));
        let settings = Settings::from_json(&json!({"data_dir": dir.to_string_lossy(), "default_peer_count": 2}));

        let mut sim = Sim::new(settings.clone(), "", rt.handle().clone());
        sim.torrents_updated(vec![torrent(&a), torrent(&b)]);
        assert_eq!((is_on(&sim, &a), is_on(&sim, &b)), (Some(false), Some(false)), "nothing is simulated by default");
        assert!(sim.jobs.is_empty());

        sim.selection_changed(&a, true, None).unwrap();
        sim.selection_changed(&b, true, Some(torrent(&b))).unwrap();
        let row = sim.selections().into_iter().find(|r| r["info_hash"] == a.as_str()).unwrap();
        assert_eq!(row["label"], "Simulating: seed_only, 2 peers");
        assert_eq!(sim.jobs.len(), 2);

        sim.selection_changed(&a, false, None).unwrap();
        assert_eq!((is_on(&sim, &a), is_on(&sim, &b)), (Some(false), Some(true)), "b is untouched");
        assert_eq!(sim.jobs.len(), 1);
        sim.shutdown();

        // a new process: b was on, a was off
        let mut sim = Sim::new(settings, "", rt.handle().clone());
        assert!(sim.jobs.is_empty(), "jobs wait for the torrent list");
        sim.torrents_updated(vec![torrent(&a), torrent(&b)]);
        assert_eq!((is_on(&sim, &a), is_on(&sim, &b)), (Some(false), Some(true)));
        assert_eq!(sim.jobs.keys().collect::<Vec<_>>(), [&b]);

        sim.torrents_updated(vec![torrent(&a)]);
        assert!(sim.jobs.is_empty(), "a torrent Drift dropped stops, its switch is kept");
        assert_eq!(is_on(&sim, &b), Some(true));
        sim.shutdown();
    }

    #[test]
    fn bad_hash_is_rejected() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!("tracker-sim-jobs-{}", crate::rng::entropy()));
        let mut sim = Sim::new(Settings::from_json(&json!({"data_dir": dir.to_string_lossy()})), "", rt.handle().clone());
        assert!(sim.selection_changed("zz", true, None).is_err());
    }

    #[test]
    fn origin_hides_the_path() {
        assert_eq!(origin("http://t.example:6969/ann?passkey=1"), "http://t.example:6969");
    }
}
