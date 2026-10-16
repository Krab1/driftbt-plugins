//! Long-term behaviour (Phase 4): a person's connection is not equally busy at 4 a.m. and at 3 p.m., and their habits
//! drift from day to day. This gives a multiplier for the configured rate band, derived from the identity's seed and
//! the clock, so it needs no stored state and is the same after a restart.

use crate::rng::Rng;

/// The factor for an identity at unix time `now`: a day-to-day drift of +-15 % and, optionally, a daily curve that
/// peaks mid-afternoon and falls to 30 % at night, shifted by a per-identity offset so identities are not in step.
pub fn factor(seed: u64, now: u64) -> f64 {
    let day = now / 86_400;
    let frac = (now % 86_400) as f64 / 86_400.0;
    let of_day = |d: u64| Rng::new(seed ^ d.wrapping_mul(0x9E37_79B9_7F4A_7C15)).f64();
    let drift = 0.85 + 0.3 * (of_day(day) * (1.0 - frac) + of_day(day + 1) * frac);
    let hour = (now as f64 / 3600.0 + (seed % 24) as f64) % 24.0;
    let curve = 0.65 + 0.35 * (std::f64::consts::TAU * (hour - 15.0) / 24.0).cos();
    drift * curve
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_in_range_varies_and_is_repeatable() {
        let day = 86_400;
        let samples: Vec<f64> = (0..24 * 7).map(|h| factor(42, 1_800_000_000 + h * 3600)).collect();
        assert!(samples.iter().all(|f| (0.25..=1.16).contains(f)), "{samples:?}");
        let (lo, hi) = samples.iter().fold((9.0f64, 0.0f64), |(l, h), &f| (l.min(f), h.max(f)));
        assert!(lo < 0.5 && hi > 0.8, "a daily swing: {lo} .. {hi}");
        assert_eq!(factor(42, 1_800_000_000), factor(42, 1_800_000_000));
        assert_ne!(factor(42, 1_800_000_000), factor(43, 1_800_000_000), "identities differ");
        assert!((factor(7, day) - factor(7, day + 1)).abs() < 0.05, "no jumps from one second to the next");
    }
}
