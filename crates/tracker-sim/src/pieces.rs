//! Piece bookkeeping: a bitfield and the strategies that choose which piece to request next.

use crate::rng::Rng;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bits {
    n: usize,
    v: Vec<u8>,
}

impl Bits {
    pub fn new(n: usize) -> Self {
        Self { n, v: vec![0; n.div_ceil(8)] }
    }

    pub fn full(n: usize) -> Self {
        let mut b = Self::new(n);
        (0..n).for_each(|i| b.set(i));
        b
    }

    /// From the wire: the length must match and the spare bits must be zero.
    pub fn from_bytes(n: usize, bytes: &[u8]) -> Option<Self> {
        let ok = bytes.len() == n.div_ceil(8) && (n.is_multiple_of(8) || bytes.last().is_none_or(|l| l & (0xffu8 >> (n % 8)) == 0));
        ok.then(|| Self { n, v: bytes.to_vec() })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.v
    }

    pub fn get(&self, i: usize) -> bool {
        i < self.n && self.v[i / 8] & (0x80 >> (i % 8)) != 0
    }

    pub fn set(&mut self, i: usize) {
        if i < self.n {
            self.v[i / 8] |= 0x80 >> (i % 8);
        }
    }

    pub fn count(&self) -> usize {
        self.v.iter().map(|b| b.count_ones() as usize).sum()
    }

    /// Whether `other` has a piece this one lacks (a byte at a time, so cheap on big torrents).
    pub fn lacks_any_of(&self, other: &Bits) -> bool {
        self.v.iter().zip(&other.v).any(|(mine, theirs)| theirs & !mine != 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Sequential,
    Random,
    RarestFirst,
    /// Rarest first, and in the last pieces ask a second peer for a piece already requested elsewhere.
    EndgameBias,
}

impl Strategy {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "sequential" => Self::Sequential,
            "random" => Self::Random,
            "rarest_first" => Self::RarestFirst,
            "endgame_bias" => Self::EndgameBias,
            _ => return None,
        })
    }
}

/// How many pieces a peer fetches at random before switching to rarest first (so it has something to trade).
const RANDOM_FIRST: usize = 4;

/// The next piece to request from a peer that has `theirs`. `requested(i)` says another connection already is
/// fetching piece `i`; `availability[i]` is how many peers we know have it.
pub fn pick(strategy: Strategy, mine: &Bits, theirs: &Bits, requested: &dyn Fn(usize) -> bool, availability: &[u16], rng: &mut Rng) -> Option<usize> {
    let wanted = |i: &usize| theirs.get(*i) && !mine.get(*i);
    let n = mine.n;
    let fresh: Vec<usize> = (0..n).filter(wanted).filter(|&i| !requested(i)).collect();
    let pool = if fresh.is_empty() && strategy == Strategy::EndgameBias { (0..n).filter(wanted).collect() } else { fresh };
    if pool.is_empty() {
        return None;
    }
    let random = |rng: &mut Rng| pool[rng.below(pool.len() as u64) as usize];
    Some(match strategy {
        Strategy::Sequential => pool[0],
        Strategy::Random => random(rng),
        Strategy::RarestFirst | Strategy::EndgameBias => {
            if mine.count() < RANDOM_FIRST {
                random(rng)
            } else {
                let rarest = pool.iter().map(|&i| availability[i]).min().unwrap_or(0);
                let ties: Vec<usize> = pool.iter().copied().filter(|&i| availability[i] == rarest).collect();
                ties[rng.below(ties.len() as u64) as usize]
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits() {
        let mut b = Bits::new(10);
        b.set(0);
        b.set(9);
        assert_eq!((b.get(0), b.get(1), b.get(9), b.count()), (true, false, true, 2));
        assert_eq!(b.as_bytes(), &[0x80, 0x40]);
        assert_eq!(Bits::from_bytes(10, b.as_bytes()), Some(b.clone()));
        assert!(Bits::from_bytes(10, &[0x80, 0x41]).is_none(), "spare bits must be zero");
        assert!(Bits::from_bytes(10, &[0x80]).is_none());
        assert!(Bits::new(10).lacks_any_of(&b) && !Bits::full(10).lacks_any_of(&b));
    }

    fn setup(have: &[usize], n: usize) -> (Bits, Bits) {
        let mut mine = Bits::new(n);
        have.iter().for_each(|&i| mine.set(i));
        (mine, Bits::full(n))
    }

    #[test]
    fn sequential_takes_the_lowest_missing() {
        let (mine, theirs) = setup(&[0, 1], 6);
        assert_eq!(pick(Strategy::Sequential, &mine, &theirs, &|_| false, &[1; 6], &mut Rng::new(1)), Some(2));
        assert_eq!(pick(Strategy::Sequential, &mine, &theirs, &|i| i == 2, &[1; 6], &mut Rng::new(1)), Some(3), "skips pieces being fetched");
    }

    #[test]
    fn rarest_first_prefers_scarce_pieces_once_it_has_a_few() {
        let (mine, theirs) = setup(&[0, 1, 2, 3], 8);
        let avail = [5, 5, 5, 5, 3, 1, 3, 3];
        for seed in 0..20 {
            assert_eq!(pick(Strategy::RarestFirst, &mine, &theirs, &|_| false, &avail, &mut Rng::new(seed)), Some(5));
        }
        let (empty, theirs) = setup(&[], 8);
        let picks: std::collections::BTreeSet<_> = (0..40).filter_map(|s| pick(Strategy::RarestFirst, &empty, &theirs, &|_| false, &avail, &mut Rng::new(s))).collect();
        assert!(picks.len() > 1, "the first pieces are random");
    }

    #[test]
    fn endgame_duplicates_only_for_endgame_bias() {
        let (mine, theirs) = setup(&[0, 1, 2, 3, 4, 5, 6], 8);
        let all_taken = |_: usize| true;
        assert_eq!(pick(Strategy::EndgameBias, &mine, &theirs, &all_taken, &[1; 8], &mut Rng::new(1)), Some(7));
        assert_eq!(pick(Strategy::RarestFirst, &mine, &theirs, &all_taken, &[1; 8], &mut Rng::new(1)), None);
        assert_eq!(pick(Strategy::Random, &Bits::full(8), &theirs, &|_| false, &[1; 8], &mut Rng::new(1)), None);
    }
}
