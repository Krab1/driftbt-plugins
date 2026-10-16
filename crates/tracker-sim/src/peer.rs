//! The behaviour engine for one simulated peer: counters that grow with the rate engines, and its own announce
//! schedule. Pure logic with no clock or network, so tests drive it with fake time; `jobs.rs` runs it for real.

use std::time::Duration;

use crate::identity::{Identity, Personality};
use crate::rate::RateEngine;
use crate::rng::Rng;
use crate::settings::{Mode, Settings, Stealth};
use crate::state::Counters;
use crate::swarm::Snapshot;

/// Longest slice of time one `step` accounts for. After a long pause (a slow tracker) counters carry on from
/// there instead of catching up in one lump.
const MAX_STEP: f64 = 0.5;

pub struct Peer {
    pub identity: Identity,
    mode: Mode,
    total: u64,
    have: u64,
    pub uploaded: u64,
    pub downloaded: u64,
    completed: bool,
    pub last_announce: Option<String>,
    up: RateEngine,
    down: RateEngine,
    rng: Rng,
    pers: Personality,
    base_ms: f64,
    jitter: f64,
    stealth: Stealth,
    /// Counters come from a real transfer (`measure`) instead of the rate engines (`step`).
    measured: bool,
    base_up: u64,
    base_down: u64,
}

impl Peer {
    pub fn new(identity: Identity, mode: Mode, total: u64, saved: Option<&Counters>, s: &Settings) -> Self {
        let pers = identity.personality(s.stealth.spread());
        let seed = identity.seed();
        let have = if mode.downloads() {
            let start = if mode == Mode::PartialSeed { (total as f64 * s.partial_percent / 100.0) as u64 } else { 0 };
            saved.and_then(|c| c.have_bytes).map_or(start, |h| h.min(total))
        } else {
            total // loopback_pair is not built yet and behaves like seed_only
        };
        Self {
            mode,
            total,
            have,
            uploaded: saved.map_or(0, |c| c.uploaded_bytes),
            downloaded: saved.map_or(0, |c| c.downloaded_bytes),
            completed: false,
            last_announce: saved.and_then(|c| c.last_announce_time.clone()),
            up: RateEngine::new(s.upload, seed ^ 1, &pers),
            down: RateEngine::new(s.download, seed ^ 2, &pers),
            rng: Rng::new(seed ^ crate::rng::entropy()),
            pers,
            base_ms: s.announce_base_ms,
            jitter: (s.jitter * s.stealth.jitter_scale()).min(0.9),
            stealth: s.stealth,
            measured: false,
            base_up: 0,
            base_down: 0,
            identity,
        }
    }

    pub fn left(&self) -> u64 {
        self.total - self.have
    }

    /// Switches to real counters: what is already counted is the base, the transfer adds to it.
    pub fn use_measured(&mut self) {
        self.measured = true;
        self.base_up = self.uploaded;
        self.base_down = self.downloaded;
        if !self.mode.downloads() {
            self.have = self.total;
        }
    }

    pub fn measure(&mut self, s: &Snapshot) {
        let was_missing = self.left() > 0;
        self.uploaded = self.base_up + s.uploaded;
        self.downloaded = self.base_down + s.downloaded;
        self.have = s.have_bytes.min(self.total);
        self.completed |= was_missing && self.left() == 0 && self.mode.downloads();
    }

    /// Advances the counters by `dt` seconds.
    pub fn step(&mut self, dt: f64) {
        if self.measured {
            return;
        }
        let dt = dt.clamp(0.0, MAX_STEP);
        if self.mode.downloads() && self.left() > 0 {
            let d = self.down.tick(dt).min(self.left());
            self.have += d;
            self.downloaded += d;
            self.completed |= self.left() == 0;
        }
        // A peer can only upload what it owns.
        let owned = if self.total == 0 { 0.0 } else { self.have as f64 / self.total as f64 };
        self.uploaded += (self.up.tick(dt) as f64 * owned) as u64;
    }

    pub fn set_activity(&mut self, f: f64) {
        self.up.set_activity(f);
        self.down.set_activity(f);
    }

    /// True once, after the download finished; the next announce carries `event=completed`.
    pub fn take_completed(&mut self) -> bool {
        std::mem::take(&mut self.completed)
    }

    pub fn rearm_completed(&mut self) {
        self.completed = true;
    }

    pub fn counters(&self) -> Counters {
        Counters {
            uploaded_bytes: self.uploaded,
            downloaded_bytes: self.downloaded,
            have_bytes: self.mode.downloads().then_some(self.have),
            last_announce_time: self.last_announce.clone(),
        }
    }

    /// A pause before the first announce, so peers started together don't announce together.
    pub fn initial_delay(&mut self, max_ms: u64) -> Duration {
        Duration::from_millis(if max_ms == 0 { 0 } else { self.rng.range(max_ms as f64 * 0.05, max_ms as f64) as u64 })
    }

    /// Time to the next announce: the base (or the tracker's own interval) with this peer's habits and jitter,
    /// now and then stretched.
    pub fn announce_delay(&mut self, tracker_interval: Option<u64>) -> Duration {
        self.jittered(tracker_interval.map_or(self.base_ms, |s| s as f64 * 1000.0))
    }

    /// `ms` scaled by this peer's habit and jitter (used for scrapes too), now and then stretched.
    pub fn jittered(&mut self, ms: f64) -> Duration {
        let mut ms = ms * self.pers.interval_factor * (1.0 + self.jitter * self.rng.range(-1.0, 1.0));
        if self.rng.f64() < self.stealth.long_gap_p() {
            ms *= self.rng.range(1.5, 3.0);
        }
        Duration::from_millis(ms.max(1000.0) as u64)
    }

    /// Exponential backoff after `fails` failures in a row: about 2 s, 4 s, 8 s ... up to 5 min.
    pub fn retry_delay(&mut self, fails: u32) -> Duration {
        let ms = (2000.0 * 2f64.powi(fails.saturating_sub(1).min(8) as i32)).min(300_000.0);
        Duration::from_millis((ms * self.rng.range(0.8, 1.2)) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity;
    use crate::settings::RateProfile;
    use serde_json::json;

    fn peer(mode: Mode, total: u64, seed: u64, extra: serde_json::Value) -> Peer {
        let mut cfg = json!({"upload_min_kbps": 100, "upload_max_kbps": 400, "upload_rate_profile": "random_walk",
                             "download_min_kbps": 200, "download_max_kbps": 800, "download_rate_profile": "constant_jitter"});
        cfg.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let s = Settings::from_json(&cfg);
        assert_eq!(s.upload.profile, RateProfile::RandomWalk);
        Peer::new(identity::generate(0, seed, "auto", ""), mode, total, None, &s)
    }

    #[test]
    fn seed_only_uploads_gradually_and_never_downloads() {
        let mut p = peer(Mode::SeedOnly, 1 << 40, 1, json!({}));
        let mut prev = 0;
        let mut biggest = 0;
        for _ in 0..600 {
            p.step(0.1); // 60 s
            biggest = biggest.max(p.uploaded - prev);
            prev = p.uploaded;
        }
        assert!(p.uploaded > 100 * 1024 * 50 && p.uploaded < 400 * 1024 * 62, "uploaded {}", p.uploaded);
        assert!(biggest < p.uploaded / 20, "one tick must not be a large share of the total");
        assert_eq!((p.downloaded, p.left()), (0, 0));
    }

    #[test]
    fn download_and_seed_shrinks_left_then_completes_once() {
        let total = 20 * 1024 * 1024;
        let mut p = peer(Mode::DownloadAndSeed, total, 2, json!({}));
        assert_eq!(p.left(), total);
        let mut last_left = total;
        let mut early_upload = None;
        for i in 0..2000 {
            p.step(0.1);
            assert!(p.left() <= last_left, "left never grows");
            assert_eq!(p.downloaded, total - p.left());
            last_left = p.left();
            if i == 5 {
                early_upload = Some(p.uploaded);
            }
        }
        assert_eq!(p.left(), 0);
        assert!(p.uploaded > early_upload.unwrap(), "uploading starts once it owns pieces");
        assert!(p.take_completed() && !p.take_completed());
    }

    #[test]
    fn partial_seed_starts_with_configured_share_and_resumes_from_saved() {
        let p = peer(Mode::PartialSeed, 1000, 3, json!({"partial_availability_percent": 30}));
        assert_eq!(p.left(), 700);
        let s = Settings::default();
        let saved = Counters { uploaded_bytes: 9, downloaded_bytes: 4, have_bytes: Some(900), last_announce_time: None };
        let r = Peer::new(identity::generate(0, 3, "auto", ""), Mode::PartialSeed, 1000, Some(&saved), &s);
        assert_eq!((r.left(), r.uploaded, r.downloaded), (100, 9, 4));
    }

    #[test]
    fn announce_delays_are_jittered_and_differ_between_peers() {
        let mut a = peer(Mode::SeedOnly, 1, 10, json!({"announce_interval_base_ms": 30000}));
        let mut b = peer(Mode::SeedOnly, 1, 11, json!({"announce_interval_base_ms": 30000}));
        let da: Vec<_> = (0..20).map(|_| a.announce_delay(None)).collect();
        let db: Vec<_> = (0..20).map(|_| b.announce_delay(None)).collect();
        assert!(da.windows(2).any(|w| w[0] != w[1]), "not periodic");
        assert_ne!(da, db);
        assert!(da.iter().chain(&db).all(|d| d.as_secs() >= 10 && d.as_secs() <= 120), "{da:?}");
    }

    #[test]
    fn retry_backs_off_and_caps() {
        let mut p = peer(Mode::SeedOnly, 1, 5, json!({}));
        assert!(p.retry_delay(1) < Duration::from_millis(2500));
        assert!(p.retry_delay(3) > Duration::from_millis(5000));
        assert!(p.retry_delay(40) <= Duration::from_millis(360_000));
    }
}
