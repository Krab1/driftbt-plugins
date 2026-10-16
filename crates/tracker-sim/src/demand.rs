//! What the tracker's own numbers say about demand (Phase 4+): a seeder with nobody to upload to uploads nothing, and
//! a downloader with nobody to download from stalls. The counts include our own peers, which are subtracted first.

/// Seeders and leechers on a torrent as far as the tracker said, *excluding* our own peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Swarm {
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
}

impl Swarm {
    pub fn from_counts(complete: Option<u64>, incomplete: Option<u64>, own_seeders: u64, own_leechers: u64) -> Self {
        Self { seeders: complete.map(|c| c.saturating_sub(own_seeders)), leechers: incomplete.map(|i| i.saturating_sub(own_leechers)) }
    }

    /// Multiplier for the upload band. Nobody to upload to: none. Otherwise the leechers are shared among the seeders
    /// (us included), so a crowded torrent with many seeders gives each a small share. Unknown: no change.
    pub fn upload_factor(&self) -> f64 {
        match (self.leechers, self.seeders) {
            (None, _) => 1.0,
            (Some(0), _) => 0.0,
            (Some(l), s) => (l as f64 / (s.unwrap_or(0) + 1) as f64).clamp(0.05, 1.0),
        }
    }

    /// Multiplier for the download band. No seeder: stalled. One seeder is slow, three or more are full speed.
    pub fn download_factor(&self) -> f64 {
        match self.seeders {
            None => 1.0,
            Some(0) => 0.0,
            Some(s) => (s as f64 / 3.0).clamp(0.2, 1.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn our_own_peers_are_not_demand() {
        // the tracker lists 1 seeder and 0 leechers, and that one seeder is us
        let s = Swarm::from_counts(Some(1), Some(0), 1, 0);
        assert_eq!((s.seeders, s.leechers), (Some(0), Some(0)));
        assert_eq!(s.upload_factor(), 0.0, "nobody to upload to");
        assert_eq!(Swarm::from_counts(Some(1), Some(0), 5, 5).seeders, Some(0), "never below zero");
    }

    #[test]
    fn upload_follows_leechers_per_seeder() {
        let f = |c, i| Swarm::from_counts(Some(c), Some(i), 1, 0).upload_factor();
        assert_eq!(f(1, 8), 1.0, "8 leechers and only us: full speed");
        assert!((f(5, 2) - 2.0 / 5.0).abs() < 1e-9, "2 leechers shared by 4 other seeders and us");
        assert_eq!(f(100, 1), 0.05, "a floor, not zero, while someone is downloading");
        assert_eq!(Swarm::default().upload_factor(), 1.0, "unknown means unchanged");
    }

    #[test]
    fn download_follows_seeders() {
        let f = |c| Swarm::from_counts(Some(c), None, 0, 0).download_factor();
        assert_eq!((f(0), f(1) < 0.4, f(2) > 0.6, f(3), f(50)), (0.0, true, true, 1.0, 1.0));
        assert_eq!(Swarm::default().download_factor(), 1.0);
    }
}
