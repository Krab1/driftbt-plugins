//! Client profiles: what a simulated peer's announces look like. The values mimic the client's defaults; tune them
//! here. Piece strategy and unchoke timings join this table in Phase 2 (docs/plugins/tracker-sim-design.md).

#[derive(Debug, Clone)]
pub struct Profile {
    pub prefix: String,
    pub user_agent: String,
    pub num_want: u32,
    pub port_lo: u16,
    pub port_hi: u16,
    /// Transmission writes its random id part in lower case and digits only.
    pub lower_body: bool,
    /// How often the choker re-decides who is unchoked (libtorrent 15 s, Transmission 10 s).
    pub unchoke_secs: u64,
    /// Transmission orders and names its announce parameters differently from libtorrent clients.
    pub transmission_query: bool,
}

pub const CONCRETE: [&str; 3] = ["qbittorrent_like", "transmission_like", "deluge_like"];

pub fn resolve(name: &str, custom_prefix: &str, custom_ua: &str) -> Profile {
    let p = |prefix: &str, ua: &str, num_want, port_lo, port_hi, lower_body, unchoke_secs| Profile { prefix: prefix.into(), user_agent: ua.into(), num_want, port_lo, port_hi, lower_body, unchoke_secs, transmission_query: false };
    match name {
        "transmission_like" => Profile { transmission_query: true, ..p("-TR4040-", "Transmission/4.0.4", 80, 51413, 51413, true, 10) },
        "deluge_like" => p("-DE211s-", "Deluge/2.1.1 libtorrent/2.0.9.0", 200, 6881, 6891, false, 15),
        "debug_obvious" => p("-DRSIM0-", "tracker-sim-debug", 50, 6881, 6881, false, 5),
        "custom" => {
            let prefix: String = custom_prefix.chars().filter(char::is_ascii_graphic).take(8).collect();
            let prefix = if prefix.is_empty() { "-qB4650-".to_string() } else { prefix };
            let ua = if custom_ua.is_empty() { "qBittorrent/4.6.5" } else { custom_ua };
            p(&prefix, ua, 200, 10_000, 65_000, false, 15)
        }
        _ => p("-qB4650-", "qBittorrent/4.6.5", 200, 10_000, 65_000, false, 15),
    }
}
