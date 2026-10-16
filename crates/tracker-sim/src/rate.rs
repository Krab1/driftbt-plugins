//! Byte-rate generator for one direction of one peer. Call `tick` every ~100-250 ms; it returns the bytes to count
//! this tick. A rate profile moves the current rate; a token bucket (capped at 2 s of the maximum rate, and at
//! 1.5x the maximum per tick) turns it into bytes, so counters grow smoothly and never jump.

use crate::identity::Personality;
use crate::rng::Rng;
use crate::settings::{RateCfg, RateProfile};

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Normal,
    Burst,
    Idle,
}

fn clamp(cfg: &RateCfg, r: f64) -> f64 {
    r.clamp(cfg.min_bps, cfg.max_bps)
}

pub struct RateEngine {
    base: RateCfg,
    cfg: RateCfg,
    rng: Rng,
    scale: f64,
    idle_prob: f64,
    burst_prob: f64,
    cur: f64,
    phase: Phase,
    phase_left: f64,
    tokens: f64,
}

impl RateEngine {
    pub fn new(cfg: RateCfg, seed: u64, p: &Personality) -> Self {
        let mid = (cfg.min_bps + cfg.max_bps) / 2.0;
        Self { base: cfg, cfg, rng: Rng::new(seed), scale: p.rate_scale, idle_prob: p.idle_prob, burst_prob: p.burst_prob, cur: mid, phase: Phase::Normal, phase_left: 0.0, tokens: 0.0 }
    }

    /// Scales the whole band (the configured min and max are for full activity); the next tick follows it.
    pub fn set_activity(&mut self, f: f64) {
        self.cfg.min_bps = self.base.min_bps * f;
        self.cfg.max_bps = self.base.max_bps * f;
    }

    fn update_rate(&mut self, dt: f64) {
        let (min, max) = (self.cfg.min_bps, self.cfg.max_bps);
        let mid = (min + max) / 2.0;
        match self.cfg.profile {
            RateProfile::ConstantJitter => {
                let target = clamp(&self.cfg, mid * self.scale * (1.0 + 0.15 * self.rng.range(-1.0, 1.0)));
                self.cur += (target - self.cur) * 0.3;
            }
            RateProfile::RandomWalk => {
                let step = self.rng.range(-1.0, 1.0) * 0.4 * (max - min) * dt.sqrt();
                self.cur = clamp(&self.cfg, self.cur + step + 0.05 * (mid * self.scale - self.cur) * dt);
            }
            RateProfile::Bursty => {
                self.phase_left -= dt;
                if self.phase_left <= 0.0 {
                    let r = self.rng.f64();
                    (self.phase, self.phase_left) = if r < self.idle_prob {
                        (Phase::Idle, self.rng.range(2.0, 15.0))
                    } else if r < self.idle_prob + self.burst_prob {
                        (Phase::Burst, self.rng.range(1.0, 6.0))
                    } else {
                        (Phase::Normal, self.rng.range(3.0, 20.0))
                    };
                }
                let target = match self.phase {
                    Phase::Idle => 0.0,
                    Phase::Burst => clamp(&self.cfg, max * self.rng.range(0.8, 1.0)),
                    Phase::Normal => clamp(&self.cfg, self.rng.range(min, mid) * self.scale),
                };
                self.cur += (target - self.cur) * (1.0 - (-dt / 0.5).exp());
            }
        }
    }

    pub fn tick(&mut self, dt: f64) -> u64 {
        if self.cfg.max_bps <= 0.0 || dt <= 0.0 {
            return 0;
        }
        self.update_rate(dt);
        self.tokens = (self.tokens + self.cur * dt).min(self.cfg.max_bps * 2.0);
        let n = self.tokens.min(self.cfg.max_bps * dt * 1.5).floor();
        self.tokens -= n;
        n as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(profile: RateProfile, seed: u64) -> RateEngine {
        let p = Personality { rate_scale: 1.0, idle_prob: 0.15, burst_prob: 0.2, interval_factor: 1.0 };
        RateEngine::new(RateCfg { profile, min_bps: 100_000.0, max_bps: 500_000.0 }, seed, &p)
    }

    #[test]
    fn stays_within_bounds_and_never_dumps() {
        for profile in [RateProfile::ConstantJitter, RateProfile::RandomWalk, RateProfile::Bursty] {
            let mut e = engine(profile, 9);
            let (mut total, mut max_tick) = (0u64, 0u64);
            for _ in 0..3000 {
                let n = e.tick(0.1);
                max_tick = max_tick.max(n);
                total += n;
            }
            // 300 s at no more than the maximum rate, plus the 2 s bucket
            assert!(total as f64 <= 500_000.0 * 300.0 + 1_000_000.0, "{profile:?} total {total}");
            assert!(max_tick as f64 <= 500_000.0 * 0.1 * 1.5, "{profile:?} tick {max_tick}");
            if profile != RateProfile::Bursty {
                assert!(total as f64 >= 100_000.0 * 295.0, "{profile:?} total {total}");
            }
        }
    }

    #[test]
    fn bursty_has_quiet_stretches() {
        let mut e = engine(RateProfile::Bursty, 3);
        let ticks: Vec<u64> = (0..6000).map(|_| e.tick(0.1)).collect();
        assert!(ticks.iter().any(|&n| n < 5_000), "expected near-idle ticks");
        assert!(ticks.iter().any(|&n| n > 40_000), "expected busy ticks");
    }

    #[test]
    fn zero_max_means_no_traffic() {
        let p = Personality { rate_scale: 1.0, idle_prob: 0.1, burst_prob: 0.1, interval_factor: 1.0 };
        let mut e = RateEngine::new(RateCfg { profile: RateProfile::RandomWalk, min_bps: 0.0, max_bps: 0.0 }, 1, &p);
        assert_eq!((0..100).map(|_| e.tick(0.1)).sum::<u64>(), 0);
    }
}
