//! The real plugin program, driven the way Drift drives it (JSON lines on stdin/stdout) against a stub HTTP tracker
//! on localhost: selection, gradual counters, jittered announces, stopped events, and persistence across a restart.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const EXE: &str = env!("CARGO_BIN_EXE_tracker-sim");
const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
const OTHER: &str = "89abcdef0123456789abcdef0123456789abcdef";

struct Plugin {
    child: Child,
    stdin: ChildStdin,
    out: BufReader<ChildStdout>,
}

impl Plugin {
    fn start() -> Self {
        let mut cmd = Command::new(EXE);
        cmd.env_clear().stdin(Stdio::piped()).stdout(Stdio::piped());
        // As Drift does: the system root keeps sockets working on Windows.
        if let Ok(root) = std::env::var("SystemRoot") {
            cmd.env("SystemRoot", root);
        }
        let mut child = cmd.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let out = BufReader::new(child.stdout.take().unwrap());
        Self { child, stdin, out }
    }

    fn send(&mut self, v: Value) {
        writeln!(self.stdin, "{v}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn call(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let mut line = String::new();
            assert!(self.out.read_line(&mut line).unwrap() > 0, "the plugin ended without answering {method}");
            let v: Value = serde_json::from_str(&line).unwrap();
            if v["method"] == "get_secret" {
                self.send(json!({"jsonrpc": "2.0", "id": v["id"], "result": {"value": null}}));
            } else if v["id"] == id {
                return v;
            }
        }
    }

    fn finish(mut self) {
        assert_eq!(self.call(99, "shutdown", json!({}))["result"], json!({}));
        assert!(self.child.wait().unwrap().success());
    }
}

struct Hit {
    at: Instant,
    query: Vec<(String, String)>,
}

impl Hit {
    fn get(&self, k: &str) -> &str {
        &self.query.iter().find(|(n, _)| n == k).unwrap_or_else(|| panic!("no {k} in {:?}", self.query)).1
    }
    /// The event, or "" for a periodic announce.
    fn event(&self) -> &str {
        self.query.iter().find(|(n, _)| n == "event").map_or("", |(_, v)| v)
    }
    fn n(&self, k: &str) -> u64 {
        self.get(k).parse().unwrap()
    }
}

/// A tracker that answers every announce with an empty peer list and reports what it was asked.
fn stub_tracker() -> (u16, mpsc::Receiver<Hit>) {
    let l = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = l.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { continue };
            let mut buf = vec![0u8; 8192];
            let n = s.read(&mut buf).unwrap_or(0);
            let first = String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_string();
            let query = first.split(' ').nth(1).and_then(|t| t.split_once('?')).map_or("", |(_, q)| q).to_string();
            let query = query.split('&').filter_map(|p| p.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect();
            let _ = tx.send(Hit { at: Instant::now(), query });
            let body = "d8:intervali1800e5:peers0:e";
            let _ = write!(s, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        }
    });
    (port, rx)
}

fn drain(rx: &mpsc::Receiver<Hit>, wait: Duration) -> Vec<Hit> {
    let end = Instant::now() + wait;
    let mut hits = Vec::new();
    while let Some(left) = end.checked_duration_since(Instant::now()) {
        if let Ok(h) = rx.recv_timeout(left) {
            hits.push(h);
        }
    }
    hits
}

fn torrent(hash: &str, port: u16) -> Value {
    json!({"info_hash": hash, "name": "ubuntu.iso", "status": "downloading", "total_size": 50_000_000_000u64, "have_bytes": 0,
           "uploaded_bytes": 0, "downloaded_bytes": 0, "trackers": [format!("http://127.0.0.1:{port}/announce")]})
}

fn settings(dir: &std::path::Path) -> Value {
    json!({"data_dir": dir.to_string_lossy(), "announce_interval_base_ms": 1500, "announce_jitter_percent": 30, "start_delay_max_ms": 800,
           "upload_rate_profile": "constant_jitter", "upload_min_kbps": 200, "upload_max_kbps": 400, "log_level": "debug"})
}

#[test]
fn selected_torrent_announces_gradually_and_survives_a_restart() {
    let dir = std::env::temp_dir().join(format!("tracker-sim-it-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (port, rx) = stub_tracker();

    // First run: switch one torrent on, let it announce for a while, switch it off.
    let mut p = Plugin::start();
    let init = p.call(1, "initialize", json!({"api": 1, "app_version": "1", "settings": settings(&dir)}));
    assert_eq!(init["result"]["kinds"], json!(["simulator"]));
    p.send(json!({"jsonrpc": "2.0", "method": "torrents_updated", "params": {"torrents": [torrent(HASH, port), torrent(OTHER, port)]}}));
    let sel = p.call(10, "get_selection", json!({}));
    assert!(sel["result"]["selections"].as_array().unwrap().iter().all(|s| s["enabled"] == false), "off by default");
    assert!(drain(&rx, Duration::from_millis(1500)).is_empty(), "nothing is announced before the user switches one on");

    assert_eq!(p.call(11, "selection_changed", json!({"info_hash": HASH, "enabled": true, "torrent": torrent(HASH, port)}))["result"], json!({}));
    let sel = p.call(12, "get_selection", json!({}));
    let row = sel["result"]["selections"].as_array().unwrap().iter().find(|s| s["info_hash"] == HASH).unwrap().clone();
    assert_eq!((row["enabled"].clone(), row["label"].clone()), (json!(true), json!("Simulating: seed_only, 1 peer")));

    let first = drain(&rx, Duration::from_secs(9));
    assert!(first.len() >= 4, "expected several announces, got {}", first.len());
    assert!(first.iter().all(|h| h.get("info_hash").starts_with("%01%23Eg%89%AB%CD%EF") && h.n("left") == 0 && h.n("downloaded") == 0));
    assert_eq!(first[0].event(), "started");
    assert_eq!(first[0].n("uploaded"), 0, "a fresh identity starts at zero");
    let up: Vec<u64> = first.iter().map(|h| h.n("uploaded")).collect();
    assert!(up.windows(2).all(|w| w[1] >= w[0]), "cumulative: {up:?}");
    let last = *up.last().unwrap();
    assert!(last > 200 * 1024 * 3, "uploaded {last} after ~9 s at >= 200 KB/s");
    let steps: Vec<u64> = up.windows(2).map(|w| w[1] - w[0]).collect();
    assert!(*steps.iter().max().unwrap() < last * 6 / 10, "no single announce carries most of the total: {up:?}");
    let gaps: Vec<u128> = first.windows(2).map(|w| w[1].at.duration_since(w[0].at).as_millis()).collect();
    assert!(gaps.iter().max().unwrap() - gaps.iter().min().unwrap() > 100, "announces are not evenly spaced: {gaps:?}");
    let peer_id = first[0].get("peer_id").to_string();
    assert!(!peer_id.contains("SIM") && peer_id.len() >= 20);

    // Switching it off sends `stopped`; the other torrent was never touched.
    p.call(13, "selection_changed", json!({"info_hash": HASH, "enabled": false, "torrent": torrent(HASH, port)}));
    let tail = drain(&rx, Duration::from_millis(1500));
    let stopped = tail.iter().find(|h| h.event() == "stopped").expect("a stopped event");
    assert!(stopped.n("uploaded") >= last);
    assert_eq!(stopped.get("numwant"), "0");
    let stopped_before = stopped.n("uploaded");
    p.call(14, "selection_changed", json!({"info_hash": HASH, "enabled": true, "torrent": torrent(HASH, port)}));
    let running = drain(&rx, Duration::from_millis(2500));
    assert!(running.iter().any(|h| h.event() == "started"), "switching it on again starts it again");
    p.finish();
    let after_shutdown = drain(&rx, Duration::from_millis(500));
    let stopped = after_shutdown.iter().find(|h| h.event() == "stopped").expect("shutdown also sends stopped");
    assert!(stopped.n("uploaded") >= stopped_before);
    let stopped_up = stopped.n("uploaded");

    // Second run: the switch, the peer id and the counters come back.
    let mut p = Plugin::start();
    p.call(1, "initialize", json!({"api": 1, "app_version": "1", "settings": settings(&dir)}));
    p.send(json!({"jsonrpc": "2.0", "method": "torrents_updated", "params": {"torrents": [torrent(HASH, port), torrent(OTHER, port)]}}));
    let sel = p.call(2, "get_selection", json!({}));
    let on: Vec<&Value> = sel["result"]["selections"].as_array().unwrap().iter().filter(|s| s["enabled"] == true).collect();
    assert_eq!(on.len(), 1);
    let second = drain(&rx, Duration::from_secs(3));
    let started = second.iter().find(|h| h.event() == "started").expect("started after restart");
    assert_eq!(started.get("peer_id"), peer_id, "same identity");
    assert!(started.n("uploaded") >= stopped_up, "counters continue instead of resetting");
    p.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bad_requests_get_valid_replies() {
    let dir = std::env::temp_dir().join(format!("tracker-sim-bad-{}", std::process::id()));
    let mut p = Plugin::start();
    p.call(5, "initialize", json!({"api": 1, "app_version": "1", "settings": {"data_dir": dir.to_string_lossy()}}));
    let r = p.call(1, "selection_changed", json!({"info_hash": "nope", "enabled": true}));
    assert_eq!(r["error"]["code"], -32602);
    let r = p.call(2, "no_such_method", json!({}));
    assert_eq!(r["error"]["code"], -32601);
    p.send(json!("garbage"));
    p.stdin.write_all(b"not json\n").unwrap();
    assert_eq!(p.call(3, "get_selection", json!({}))["result"]["selections"], json!([]));
    p.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn per_peer_proxy_routes_announces_and_a_peer_without_proxy_stays_quiet() {
    let dir = std::env::temp_dir().join(format!("tracker-sim-proxy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    // The stub plays the proxy: an HTTP proxy receives the full tracker address in the request line.
    let (proxy_port, rx) = stub_tracker();
    let mut cfg = settings(&dir);
    cfg["proxy_mode"] = json!("per_peer");
    cfg["default_peer_count"] = json!(2);
    cfg["proxies"] = json!([{"id": "p1", "url": format!("http://127.0.0.1:{proxy_port}")}]);
    let mut p = Plugin::start();
    p.call(1, "initialize", json!({"api": 1, "app_version": "1", "settings": cfg}));
    let mut t = torrent(HASH, 9);
    t["trackers"] = json!(["http://tracker.invalid/announce"]);
    p.send(json!({"jsonrpc": "2.0", "method": "torrents_updated", "params": {"torrents": [t.clone()]}}));
    p.call(2, "selection_changed", json!({"info_hash": HASH, "enabled": true, "torrent": t}));
    let hits = drain(&rx, Duration::from_secs(4));
    assert!(hits.len() >= 2, "announces reach the proxy, got {}", hits.len());
    let ids: std::collections::BTreeSet<&str> = hits.iter().map(|h| h.get("peer_id")).collect();
    assert_eq!(ids.len(), 1, "peer 2 has no proxy and must not announce from the real address: {ids:?}");
    p.finish();
    let state = std::fs::read_to_string(dir.join("state.json")).unwrap();
    assert!(state.contains("\"proxy_id\": \"p1\""), "the assignment is saved");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn loopback_pair_announces_bytes_that_really_moved() {
    const TOTAL: u64 = 3_000_000;
    let dir = std::env::temp_dir().join(format!("tracker-sim-loop-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (port, rx) = stub_tracker();
    let mut cfg = settings(&dir);
    cfg["default_mode"] = json!("loopback_pair");
    cfg["upload_min_kbps"] = json!(2000);
    cfg["upload_max_kbps"] = json!(4000);
    cfg["download_rate_profile"] = json!("constant_jitter");
    cfg["download_min_kbps"] = json!(2000);
    cfg["download_max_kbps"] = json!(4000);
    let mut p = Plugin::start();
    p.call(1, "initialize", json!({"api": 1, "app_version": "1", "settings": cfg}));
    let mut t = torrent(HASH, port);
    t["total_size"] = json!(TOTAL);
    p.send(json!({"jsonrpc": "2.0", "method": "torrents_updated", "params": {"torrents": [t.clone()]}}));
    p.call(2, "selection_changed", json!({"info_hash": HASH, "enabled": true, "torrent": t}));
    let hits = drain(&rx, Duration::from_secs(9));
    let sel = p.call(3, "get_selection", json!({}));
    assert_eq!(sel["result"]["selections"][0]["label"], "Simulating: loopback_pair, 2 peers");
    p.finish();

    let mut ids: Vec<&str> = hits.iter().map(|h| h.get("peer_id")).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 2, "a seeder and a leecher announce");
    let of = |id: &str| hits.iter().filter(|h| h.get("peer_id") == id && h.event() != "stopped").collect::<Vec<_>>();
    let (a, b) = (of(ids[0]), of(ids[1]));
    let (seed, leech) = if a[0].n("left") == 0 { (a, b) } else { (b, a) };
    assert!(seed.iter().all(|h| h.n("left") == 0 && h.n("downloaded") == 0));
    assert_eq!(leech[0].n("left"), TOTAL, "the leecher starts with nothing");
    assert!(leech.iter().any(|h| h.event() == "completed" && h.n("left") == 0 && h.n("downloaded") == TOTAL), "completed is announced with the true totals");
    assert!(leech.windows(2).all(|w| w[1].n("left") <= w[0].n("left") && w[1].n("downloaded") >= w[0].n("downloaded")));
    assert!(seed.last().unwrap().n("uploaded") >= TOTAL, "the seeder uploaded what the leecher received");
    let _ = std::fs::remove_dir_all(&dir);
}
