//! Live numbers for the statistics window in Drift's torrent menu (`get_status`). Kept in memory only, a few lines per
//! torrent, and dropped when the torrent's simulation stops or the plugin exits.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use serde_json::{json, Value};

const MAX_LOG: usize = 40;

pub fn human(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let (mut v, mut u) = (n as f64, 0);
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 { format!("{n} B") } else { format!("{v:.1} {}", UNITS[u]) }
}

pub fn clock() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}

#[derive(Default)]
pub struct PeerStat {
    pub client: String,
    pub proxy: String,
    pub state: &'static str,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub announces: u64,
    pub next_at: Option<Instant>,
    pub last_answer: String,
}

#[derive(Default)]
pub struct TorrentStats {
    pub peers: BTreeMap<String, PeerStat>,
    pub log: VecDeque<String>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    pub interval: Option<u64>,
}

#[derive(Default)]
pub struct Stats(Mutex<HashMap<String, TorrentStats>>);

impl Stats {
    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, TorrentStats>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Begins a fresh record for a torrent whose simulation just started.
    pub fn start(&self, hash: &str) {
        self.map().insert(hash.to_string(), TorrentStats::default());
    }

    pub fn clear(&self, hash: &str) {
        self.map().remove(hash);
    }

    /// Updates a torrent's record if it has one (a peer still finishing after a stop must not bring it back).
    pub fn with(&self, hash: &str, f: impl FnOnce(&mut TorrentStats)) {
        if let Some(t) = self.map().get_mut(hash) {
            f(t);
        }
    }

    pub fn log(&self, hash: &str, line: &str) {
        self.with(hash, |t| {
            if t.log.len() >= MAX_LOG {
                t.log.pop_front();
            }
            t.log.push_back(format!("{} {line}", clock()));
        });
    }

    /// The `get_status` answer: tables and the log, newest line last.
    pub fn status(&self, hash: &str) -> Value {
        let map = self.map();
        let Some(t) = map.get(hash) else { return json!({"tables": [], "log": []}) };
        let now = Instant::now();
        let rows: Vec<Vec<String>> = t
            .peers
            .iter()
            .map(|(id, p)| {
                let next = p.next_at.map_or("-".to_string(), |at| if at > now { format!("in {} s", (at - now).as_secs()) } else { "now".into() });
                let who = if p.proxy.is_empty() { p.client.clone() } else { format!("{} via {}", p.client, p.proxy) };
                vec![id.clone(), who, p.state.to_string(), human(p.uploaded), human(p.downloaded), human(p.left), p.announces.to_string(), next, p.last_answer.clone()]
            })
            .collect();
        let mut tables = vec![json!({"title": "Peers", "columns": ["Peer", "Client", "State", "Uploaded", "Downloaded", "Left", "Announces", "Next", "Last answer"], "rows": rows})];
        let known = |label: &str, v: Option<u64>, unit: &str| v.map(|v| vec![label.to_string(), format!("{v}{unit}")]);
        let tracker_rows: Vec<Vec<String>> = [known("Seeders (not counting ours)", t.seeders, ""), known("Leechers (not counting ours)", t.leechers, ""), known("Announce interval", t.interval, " s")].into_iter().flatten().collect();
        if !tracker_rows.is_empty() {
            tables.push(json!({"title": "The tracker says", "columns": ["Item", "Value"], "rows": tracker_rows}));
        }
        json!({"tables": tables, "log": t.log})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_read_naturally() {
        assert_eq!((human(0).as_str(), human(1023).as_str(), human(1536).as_str(), human(5 << 30).as_str()), ("0 B", "1023 B", "1.5 KB", "5.0 GB"));
    }

    #[test]
    fn a_torrent_has_stats_until_cleared_and_the_log_stays_short() {
        let s = Stats::default();
        assert_eq!(s.status("a")["tables"], json!([]), "unknown torrent");
        s.start("a");
        s.with("a", |t| {
            t.peers.insert("bot-001".into(), PeerStat { client: "qbittorrent".into(), state: "active", uploaded: 2048, announces: 3, ..Default::default() });
            t.seeders = Some(4);
        });
        for i in 0..100 {
            s.log("a", &format!("line {i}"));
        }
        let v = s.status("a");
        assert_eq!(v["tables"][0]["rows"][0][3], "2.0 KB");
        assert_eq!(v["tables"][1]["rows"][0][1], "4");
        assert_eq!(v["log"].as_array().unwrap().len(), MAX_LOG);
        assert!(v["log"][MAX_LOG - 1].as_str().unwrap().ends_with("line 99"));
        s.clear("a");
        s.log("a", "late line from a peer that was still stopping");
        assert_eq!(s.status("a")["log"], json!([]), "a cleared torrent is not brought back");
    }
}
