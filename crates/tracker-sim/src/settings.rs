//! The plugin's settings form (plugin.json) as typed values. Drift sends only the values the user saved, so every
//! field has a default; a bad value falls back to it with a warning instead of failing `initialize`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::pieces::Strategy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    SeedOnly,
    PartialSeed,
    DownloadAndSeed,
    LoopbackPair,
}

impl Mode {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "seed_only" => Self::SeedOnly,
            "partial_seed" => Self::PartialSeed,
            "download_and_seed" => Self::DownloadAndSeed,
            "loopback_pair" => Self::LoopbackPair,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::SeedOnly => "seed_only",
            Self::PartialSeed => "partial_seed",
            Self::DownloadAndSeed => "download_and_seed",
            Self::LoopbackPair => "loopback_pair",
        }
    }

    /// Whether this peer receives data (and so has a `left` that shrinks).
    pub fn downloads(self) -> bool {
        matches!(self, Self::PartialSeed | Self::DownloadAndSeed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateProfile {
    ConstantJitter,
    RandomWalk,
    Bursty,
}

impl RateProfile {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "constant_jitter" => Self::ConstantJitter,
            "random_walk" => Self::RandomWalk,
            "bursty" => Self::Bursty,
            _ => return None,
        })
    }
}

/// How much human-looking irregularity is added: announce jitter, rare long gaps, spread between identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stealth {
    Basic,
    Medium,
    High,
    Maximum,
}

impl Stealth {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "basic" => Self::Basic,
            "medium" => Self::Medium,
            "high" => Self::High,
            "maximum" => Self::Maximum,
            _ => return None,
        })
    }

    pub fn jitter_scale(self) -> f64 {
        match self {
            Self::Basic => 0.5,
            _ => 1.0,
        }
    }

    /// Chance that an announce gap is stretched 1.5-3x (a client that was busy, asleep, or throttled).
    pub fn long_gap_p(self) -> f64 {
        match self {
            Self::Basic => 0.0,
            Self::Medium => 0.02,
            Self::High => 0.05,
            Self::Maximum => 0.08,
        }
    }

    /// How far personalities differ between identities.
    pub fn spread(self) -> f64 {
        match self {
            Self::Basic => 0.2,
            Self::Medium => 0.6,
            Self::High => 1.0,
            Self::Maximum => 1.4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashEncoding {
    Raw,
    Hex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyMode {
    None,
    Shared,
    PerPeer,
}

impl ProxyMode {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "none" => Self::None,
            "shared" => Self::Shared,
            "per_peer" => Self::PerPeer,
            _ => return None,
        })
    }
}

/// One row of the proxy list. The password is a secret and is filled in by `main` after `from_json`.
#[derive(Debug, Clone, PartialEq)]
pub struct ProxyDef {
    pub id: String,
    pub url: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Copy)]
pub struct RateCfg {
    pub profile: RateProfile,
    pub min_bps: f64,
    pub max_bps: f64,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub stealth: Stealth,
    pub client_profile: String,
    pub default_mode: Mode,
    pub default_peer_count: usize,
    pub announce_base_ms: f64,
    pub jitter: f64,
    pub start_delay_max_ms: u64,
    pub honor_tracker_interval: bool,
    pub upload: RateCfg,
    pub download: RateCfg,
    pub activity_pattern: bool,
    pub piece_strategy: Strategy,
    pub partial_percent: f64,
    pub persist_counters: bool,
    pub send_started: bool,
    pub scrape_enabled: bool,
    pub scrape_interval_ms: f64,
    pub info_hash_encoding: HashEncoding,
    pub api_key_param: String,
    pub extra_query: String,
    pub custom_prefix: String,
    pub custom_user_agent: String,
    pub proxy_mode: ProxyMode,
    pub proxies: Vec<ProxyDef>,
    pub local_address: Option<std::net::IpAddr>,
    pub data_dir: Option<String>,
    pub log_level: String,
}

fn text<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k)?.as_str().map(str::trim).filter(|s| !s.is_empty())
}

fn num(v: &Value, k: &str) -> Option<f64> {
    let x = v.get(k)?;
    x.as_f64().or_else(|| x.as_str()?.trim().parse().ok()).filter(|n| n.is_finite())
}

fn flag(v: &Value, k: &str, default: bool) -> bool {
    v.get(k).and_then(Value::as_bool).unwrap_or(default)
}

fn choice<T>(v: &Value, k: &str, parse: fn(&str) -> Option<T>, default: T) -> T {
    match text(v, k) {
        None => default,
        Some(s) => parse(s).unwrap_or_else(|| {
            warn!("setting {k}: unknown value {s:?}, using the default");
            default
        }),
    }
}

fn rate(v: &Value, prefix: &str, profile: RateProfile, min: f64, max: f64) -> RateCfg {
    let key = |s: &str| format!("{prefix}_{s}");
    let a = num(v, &key("min_kbps")).unwrap_or(min).clamp(0.0, 1_000_000.0);
    let b = num(v, &key("max_kbps")).unwrap_or(max).clamp(0.0, 1_000_000.0);
    RateCfg {
        profile: choice(v, &key("rate_profile"), RateProfile::parse, profile),
        min_bps: a.min(b) * 1024.0,
        max_bps: a.max(b) * 1024.0,
    }
}

fn proxies(v: &Value) -> Vec<ProxyDef> {
    let mut out: Vec<ProxyDef> = Vec::new();
    for r in v.get("proxies").and_then(Value::as_array).into_iter().flatten() {
        let (Some(id), Some(url)) = (text(r, "id"), text(r, "url")) else { continue };
        if !["socks5://", "socks5h://", "http://", "https://"].iter().any(|p| url.starts_with(p)) {
            warn!("proxy {id}: the address must start with socks5://, socks5h://, http:// or https://; skipped");
            continue;
        }
        if url.starts_with("socks5://") {
            warn!("proxy {id}: socks5:// looks up names on this computer; socks5h:// lets the proxy do it");
        }
        if out.iter().any(|p| p.id == id) {
            continue;
        }
        out.push(ProxyDef { id: id.to_string(), url: url.to_string(), username: text(r, "username").unwrap_or("").to_string(), password: String::new() });
    }
    out
}

impl Settings {
    pub fn from_json(v: &Value) -> Self {
        Self {
            stealth: choice(v, "stealth_level", Stealth::parse, Stealth::High),
            client_profile: text(v, "client_profile").unwrap_or("auto").to_string(),
            default_mode: choice(v, "default_mode", Mode::parse, Mode::SeedOnly),
            default_peer_count: num(v, "default_peer_count").unwrap_or(1.0).clamp(1.0, 20.0) as usize,
            announce_base_ms: num(v, "announce_interval_base_ms").unwrap_or(30_000.0).clamp(1000.0, 3_600_000.0),
            jitter: num(v, "announce_jitter_percent").unwrap_or(25.0).clamp(0.0, 90.0) / 100.0,
            start_delay_max_ms: num(v, "start_delay_max_ms").unwrap_or(15_000.0).clamp(0.0, 300_000.0) as u64,
            honor_tracker_interval: flag(v, "honor_tracker_interval", false),
            upload: rate(v, "upload", RateProfile::Bursty, 100.0, 800.0),
            download: rate(v, "download", RateProfile::RandomWalk, 20.0, 500.0),
            activity_pattern: flag(v, "activity_pattern", false),
            piece_strategy: choice(v, "piece_strategy", Strategy::parse, Strategy::RarestFirst),
            partial_percent: num(v, "partial_availability_percent").unwrap_or(50.0).clamp(1.0, 99.0),
            persist_counters: flag(v, "persist_counters", true),
            send_started: flag(v, "send_started_event", true),
            scrape_enabled: flag(v, "scrape_enabled", false),
            scrape_interval_ms: num(v, "scrape_interval_ms").unwrap_or(120_000.0).clamp(10_000.0, 3_600_000.0),
            info_hash_encoding: match text(v, "info_hash_encoding") {
                Some("hex") => HashEncoding::Hex,
                _ => HashEncoding::Raw,
            },
            api_key_param: text(v, "api_key_param").unwrap_or("apikey").to_string(),
            extra_query: text(v, "extra_query").unwrap_or("").trim_start_matches(['?', '&']).to_string(),
            custom_prefix: text(v, "custom_peer_id_prefix").unwrap_or("").to_string(),
            custom_user_agent: text(v, "custom_user_agent").unwrap_or("").to_string(),
            proxy_mode: choice(v, "proxy_mode", ProxyMode::parse, ProxyMode::None),
            proxies: proxies(v),
            local_address: text(v, "outbound_address").and_then(|s| s.parse().map_err(|_| warn!("setting outbound_address: {s:?} is not an IP address")).ok()),
            data_dir: text(v, "data_dir").map(str::to_string),
            log_level: text(v, "log_level").unwrap_or("info").to_string(),
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::from_json(&Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_apply() {
        let s = Settings::default();
        assert_eq!(s.default_mode, Mode::SeedOnly);
        assert_eq!(s.default_peer_count, 1);
        assert_eq!(s.upload.profile, RateProfile::Bursty);
        assert_eq!(s.upload.max_bps, 800.0 * 1024.0);
        assert!(s.persist_counters && s.send_started && !s.scrape_enabled);
    }

    #[test]
    fn bad_values_are_clamped_or_defaulted() {
        let s = Settings::from_json(&json!({
            "default_mode": "nonsense", "default_peer_count": 999, "announce_jitter_percent": "500",
            "upload_min_kbps": 900, "upload_max_kbps": 100, "announce_interval_base_ms": 5
        }));
        assert_eq!(s.default_mode, Mode::SeedOnly);
        assert_eq!(s.default_peer_count, 20);
        assert_eq!(s.jitter, 0.9);
        assert!(s.upload.min_bps < s.upload.max_bps, "min and max are put in order");
        assert_eq!(s.announce_base_ms, 1000.0);
    }

    #[test]
    fn query_prefix_is_stripped() {
        assert_eq!(Settings::from_json(&json!({"extra_query": "?passkey=abc"})).extra_query, "passkey=abc");
    }
}

#[cfg(test)]
mod proxy_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn proxy_list_is_validated() {
        let s = Settings::from_json(&json!({"proxy_mode": "per_peer", "outbound_address": "10.0.0.5", "proxies": [
            {"id": "a", "url": "socks5h://h:1080", "username": "u"}, {"id": "a", "url": "socks5://dup:1"},
            {"id": "b", "url": "ftp://x"}, {"url": "http://noid"}, {"id": "c", "url": "http://p:8080"}]}));
        assert_eq!(s.proxy_mode, ProxyMode::PerPeer);
        assert_eq!(s.proxies.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(), ["a", "c"]);
        assert_eq!(s.proxies[0].username, "u");
        assert_eq!(s.local_address, Some("10.0.0.5".parse().unwrap()));
    }
}
