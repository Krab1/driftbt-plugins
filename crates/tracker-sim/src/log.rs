//! Logging to stderr (Drift keeps it in the plugin's log file). One global level, set once from the settings.

use std::sync::atomic::{AtomicU8, Ordering};

static LEVEL: AtomicU8 = AtomicU8::new(2);

pub fn set_level(name: &str) {
    LEVEL.store(
        match name {
            "error" => 0,
            "warn" => 1,
            "debug" => 3,
            _ => 2,
        },
        Ordering::Relaxed,
    );
}

pub fn enabled(level: u8) -> bool {
    level <= LEVEL.load(Ordering::Relaxed)
}

macro_rules! error { ($($a:tt)*) => { if crate::log::enabled(0) { eprintln!("[ERROR] {}", format_args!($($a)*)) } } }
macro_rules! warn { ($($a:tt)*) => { if crate::log::enabled(1) { eprintln!("[WARN] {}", format_args!($($a)*)) } } }
macro_rules! info { ($($a:tt)*) => { if crate::log::enabled(2) { eprintln!("[INFO] {}", format_args!($($a)*)) } } }
macro_rules! debug { ($($a:tt)*) => { if crate::log::enabled(3) { eprintln!("[DEBUG] {}", format_args!($($a)*)) } } }
