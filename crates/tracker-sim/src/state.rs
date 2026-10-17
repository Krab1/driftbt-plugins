//! What survives a restart: which torrents are switched on (and how), the identities, and the per-identity,
//! per-torrent counters. One JSON file, written through a temporary file so a crash can't leave half of it.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::identity::{self, Identity};
use crate::rng;
use crate::settings::Mode;

pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TorrentSel {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// This torrent's own choices from the form in Drift's torrent menu (see `Settings::for_torrent`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Counters {
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    /// Bytes the peer owns, for the modes that download. Absent for seed-only peers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub have_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_announce_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct State {
    pub version: u32,
    #[serde(default)]
    pub identities: Vec<Identity>,
    #[serde(default)]
    pub torrents: BTreeMap<String, TorrentSel>,
    /// Keyed `<identity_id>:<info_hash>`.
    #[serde(default)]
    pub counters: BTreeMap<String, Counters>,
}

impl Default for State {
    fn default() -> Self {
        Self { version: 1, identities: Vec::new(), torrents: BTreeMap::new(), counters: BTreeMap::new() }
    }
}

pub struct Store {
    path: PathBuf,
    /// The file exists but couldn't be read: saving would replace it with an empty state, so saving is off.
    unreadable: bool,
    pub state: State,
}

impl Store {
    /// A missing file is a fresh start; an unreadable one is set aside as `state.json.corrupt` and replaced.
    pub fn load(path: PathBuf) -> Self {
        let mut unreadable = false;
        let mut read = std::fs::read_to_string(&path);
        for _ in 0..5 {
            // Antivirus and indexers hold files briefly on Windows.
            if matches!(&read, Err(e) if e.kind() != std::io::ErrorKind::NotFound) {
                std::thread::sleep(std::time::Duration::from_millis(200));
                read = std::fs::read_to_string(&path);
            }
        }
        let state = match read {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
                error!("state file {} is corrupt ({e}); keeping a copy and starting empty", path.display());
                let _ = std::fs::rename(&path, path.with_extension("json.corrupt"));
                State::default()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(e) => {
                error!("can't read state file {}: {e}; running without saving so it isn't overwritten", path.display());
                unreadable = true;
                State::default()
            }
        };
        Self { path, unreadable, state }
    }

    pub fn save(&self) {
        if self.unreadable {
            return;
        }
        let tmp = self.path.with_extension("json.tmp");
        let result = serde_json::to_vec_pretty(&self.state)
            .map_err(|e| e.to_string())
            .and_then(|b| {
                if let Some(dir) = self.path.parent() {
                    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
                }
                std::fs::File::create(&tmp).and_then(|mut f| std::io::Write::write_all(&mut f, &b).and_then(|()| f.sync_all())).and_then(|()| std::fs::rename(&tmp, &self.path)).map_err(|e| e.to_string())
            });
        if let Err(e) = result {
            error!("can't save state to {}: {e}", self.path.display());
        }
    }

    pub fn set_proxy(&mut self, identity_id: &str, proxy_id: Option<String>) {
        if let Some(id) = self.state.identities.iter_mut().find(|i| i.identity_id == identity_id).filter(|i| i.proxy_id != proxy_id) {
            id.proxy_id = proxy_id;
            self.save();
        }
    }

    /// The identity at `index`, created (and saved) on first use. Identities are numbered in order, so peer
    /// number 2 of any torrent is always bot-002.
    ///
    /// A torrent that names a client gets that client's own identities: its peer number 2 is the second identity
    /// with that profile, created when it is first needed. `auto` takes the numbered ones whatever their profile.
    pub fn identity(&mut self, index: usize, requested: &str, custom_prefix: &str) -> Identity {
        let lower = requested.to_ascii_lowercase();
        // Anything that isn't a known client is `auto`: a made-up name could never be matched and would add identities forever.
        let want = match crate::profiles::canonical(lower.trim()) {
            n @ ("qbittorrent" | "transmission" | "deluge" | "custom" | "debug_obvious") => n,
            _ => "auto",
        };
        if want != "auto" {
            let of_kind = |s: &Self| s.state.identities.iter().enumerate().filter(|(_, i)| crate::profiles::canonical(&i.client_profile) == want).map(|(n, _)| n).collect::<Vec<_>>();
            while of_kind(self).len() <= index {
                self.add_identity(want, custom_prefix);
            }
            return self.state.identities[of_kind(self)[index]].clone();
        }
        while self.state.identities.len() <= index {
            self.add_identity(requested, custom_prefix);
        }
        self.state.identities[index].clone()
    }

    fn add_identity(&mut self, requested: &str, custom_prefix: &str) {
        let mut id = identity::generate(self.state.identities.len(), rng::entropy(), requested, custom_prefix);
        // A tracker keys peers by address and port, so two identities sharing a port would be one peer to it.
        while self.state.identities.iter().any(|o| o.port == id.port) {
            id.port = if id.port == u16::MAX { 10_000 } else { id.port + 1 };
        }
        info!("new identity {} peer_id={} port={} profile={}", id.identity_id, id.peer_id, id.port, id.client_profile);
        self.state.identities.push(id);
        self.save();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tracker-sim-{name}-{}", rng::entropy()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("state.json")
    }

    #[test]
    fn roundtrip() {
        let path = temp("roundtrip");
        let mut s = Store::load(path.clone());
        let id = s.identity(1, "auto", "");
        assert_eq!(s.state.identities.len(), 2, "creating bot-002 also creates bot-001");
        s.state.torrents.insert("ab".repeat(20), TorrentSel { enabled: true, mode: Some(Mode::SeedOnly), peer_count: Some(2), started_at: Some(now_iso()), options: None });
        s.state.counters.insert("bot-001:x".into(), Counters { uploaded_bytes: 5, ..Default::default() });
        s.save();
        let back = Store::load(path);
        assert_eq!(back.state, s.state);
        assert_eq!(back.state.identities[1], id);
    }

    #[test]
    fn identities_never_share_a_port() {
        let mut s = Store::load(temp("ports"));
        let ports: std::collections::BTreeSet<u16> = (0..8).map(|i| s.identity(i, "transmission", "").port).collect();
        assert_eq!(ports.len(), 8, "Transmission's fixed port 51413 must not repeat");
    }

    #[test]
    fn corrupt_file_starts_empty_and_is_kept() {
        let path = temp("corrupt");
        std::fs::write(&path, "{ nope").unwrap();
        let s = Store::load(path.clone());
        assert!(s.state.torrents.is_empty());
        assert!(path.with_extension("json.corrupt").exists());
    }

    #[test]
    fn disabled_entry_serialises_as_just_enabled() {
        let v = serde_json::to_value(TorrentSel::default()).unwrap();
        assert_eq!(v, serde_json::json!({"enabled": false}));
    }
}

#[cfg(test)]
mod client_tests {
    use super::*;

    #[test]
    fn an_unknown_client_name_means_auto_instead_of_looping() {
        let mut s = Store::load(std::env::temp_dir().join(format!("tracker-sim-cl-{}", rng::entropy())).join("state.json"));
        let a = s.identity(1, "Utorrent", "");
        assert_eq!(s.state.identities.len(), 2, "no runaway identities");
        assert_eq!(s.identity(1, "  ", ""), a);
        let t = s.identity(0, "Transmission", "");
        assert_eq!(t.client_profile, "transmission", "names are not case sensitive");
    }

    #[test]
    fn a_proxy_is_saved_on_the_identity_it_was_given_to() {
        let mut s = Store::load(std::env::temp_dir().join(format!("tracker-sim-cl-{}", rng::entropy())).join("state.json"));
        let _auto = s.identity(2, "auto", "");
        let tr = s.identity(0, "transmission", "");
        s.set_proxy(&tr.identity_id, Some("p1".into()));
        assert_eq!(s.state.identities.iter().filter(|i| i.proxy_id.is_some()).map(|i| i.identity_id.clone()).collect::<Vec<_>>(), vec![tr.identity_id]);
    }

    #[test]
    fn a_named_client_has_its_own_peers() {
        let mut s = Store::load(std::env::temp_dir().join(format!("tracker-sim-cl-{}", rng::entropy())).join("state.json"));
        let a = s.identity(0, "transmission", "");
        let b = s.identity(1, "transmission", "");
        let q = s.identity(0, "qbittorrent", "");
        assert!(a.peer_id.starts_with("-TR4040-") && b.peer_id.starts_with("-TR4040-") && q.peer_id.starts_with("-qB4650-"));
        assert_ne!(a.identity_id, b.identity_id);
        assert_eq!(s.identity(1, "transmission", ""), b, "the same peer number is the same peer next time");
        assert_eq!(s.identity(0, "transmission", ""), a);
        let ports: std::collections::BTreeSet<u16> = [&a, &b, &q].iter().map(|i| i.port).collect();
        assert_eq!(ports.len(), 3, "no two share a port");
    }
}
