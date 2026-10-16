//! Stable simulated identities. An identity is created once, saved in state.json and reused for every torrent and
//! every run, so a tracker sees the same peer id, port and key from the same "client" over time.

use serde::{Deserialize, Serialize};

use crate::profiles;
use crate::rng::Rng;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    pub identity_id: String,
    pub peer_id: String,
    pub key: String,
    pub port: u16,
    pub client_profile: String,
    pub personality_seed: String,
    /// The proxy this identity always uses (Phase 3), kept so it does not change address between runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_id: Option<String>,
}

/// Behaviour parameters derived from the stored seed, so an identity acts the same way after a restart.
#[derive(Debug, Clone, Copy)]
pub struct Personality {
    pub rate_scale: f64,
    pub idle_prob: f64,
    pub burst_prob: f64,
    pub interval_factor: f64,
}

const ALNUM: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// `requested` is the client_profile setting; `auto` picks one of the three real-client looks from the seed.
pub fn generate(index: usize, seed: u64, requested: &str, custom_prefix: &str) -> Identity {
    let mut rng = Rng::new(seed);
    let name = match requested {
        "qbittorrent_like" | "transmission_like" | "deluge_like" | "custom" | "debug_obvious" => requested.to_string(),
        _ => profiles::CONCRETE[match rng.below(10) {
            0..=4 => 0,
            5..=7 => 1,
            _ => 2,
        }]
        .to_string(),
    };
    let profile = profiles::resolve(&name, custom_prefix, "");
    let pool = if profile.lower_body { &ALNUM[..36] } else { ALNUM };
    let mut peer_id = profile.prefix.clone();
    while peer_id.len() < 20 {
        peer_id.push(pool[rng.below(pool.len() as u64) as usize] as char);
    }
    let port = profile.port_lo + rng.below(u64::from(profile.port_hi - profile.port_lo) + 1) as u16;
    Identity {
        identity_id: format!("bot-{:03}", index + 1),
        peer_id,
        key: format!("{:08X}", rng.next_u64() as u32),
        port,
        client_profile: name,
        personality_seed: format!("{:016x}", rng.next_u64()),
        proxy_id: None,
    }
}

impl Identity {
    pub fn seed(&self) -> u64 {
        u64::from_str_radix(&self.personality_seed, 16).unwrap_or(1)
    }

    pub fn personality(&self, spread: f64) -> Personality {
        let mut r = Rng::new(self.seed() ^ 0xA5A5);
        Personality {
            rate_scale: r.range(1.0 - 0.3 * spread, 1.0 + 0.3 * spread).max(0.3),
            idle_prob: r.range(0.05, 0.20),
            burst_prob: r.range(0.10, 0.30),
            interval_factor: r.range(1.0 - 0.15 * spread, 1.0 + 0.15 * spread).max(0.5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_identity() {
        assert_eq!(generate(0, 42, "auto", ""), generate(0, 42, "auto", ""));
        assert_ne!(generate(0, 42, "auto", "").peer_id, generate(0, 43, "auto", "").peer_id);
    }

    #[test]
    fn peer_id_is_20_ascii_and_stealth_profiles_have_no_sim_prefix() {
        for seed in 0..50 {
            let id = generate(seed as usize, seed, "auto", "");
            assert_eq!(id.peer_id.len(), 20);
            assert!(id.peer_id.is_ascii());
            assert!(!id.peer_id.contains("SIM"));
        }
        assert!(generate(0, 1, "debug_obvious", "").peer_id.starts_with("-DRSIM0-"));
    }

    #[test]
    fn personality_is_stable_and_ids_are_numbered() {
        let id = generate(2, 7, "qbittorrent_like", "");
        assert_eq!(id.identity_id, "bot-003");
        assert_eq!(id.personality(1.0).rate_scale, id.personality(1.0).rate_scale);
    }
}
