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

    pub fn set_proxy(&mut self, index: usize, proxy_id: Option<String>) {
        if let Some(id) = self.state.identities.get_mut(index).filter(|i| i.proxy_id != proxy_id) {
            id.proxy_id = proxy_id;
            self.save();
        }
    }

    /// The identity at `index`, created (and saved) on first use. Identities are numbered in order, so peer
    /// number 2 of any torrent is always bot-002.
    pub fn identity(&mut self, index: usize, requested: &str, custom_prefix: &str) -> Identity {
        while self.state.identities.len() <= index {
            let id = identity::generate(self.state.identities.len(), rng::entropy(), requested, custom_prefix);
            info!("new identity {} peer_id={} port={} profile={}", id.identity_id, id.peer_id, id.port, id.client_profile);
            self.state.identities.push(id);
            self.save();
        }
        self.state.identities[index].clone()
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
        s.state.torrents.insert("ab".repeat(20), TorrentSel { enabled: true, mode: Some(Mode::SeedOnly), peer_count: Some(2), started_at: Some(now_iso()) });
        s.state.counters.insert("bot-001:x".into(), Counters { uploaded_bytes: 5, ..Default::default() });
        s.save();
        let back = Store::load(path);
        assert_eq!(back.state, s.state);
        assert_eq!(back.state.identities[1], id);
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
