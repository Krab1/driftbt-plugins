# tracker-sim: design

A Drift `simulator` plugin (`crates/tracker-sim`, binary `tracker-sim`). For each torrent the user switches on, it announces to the torrent's trackers as stable, human-looking peers whose counters grow gradually. **Purpose: testing a tracker and its anti-bot checks that you operate or have permission to test.** It is not a way to inflate ratios on someone else's tracker; the manifest description says so. Protocol and the host side are in `docs/PLUGIN_PROTOCOL.md` of the Drift repo (Krab1/driftbt) ("Simulators"); this file is only the plugin.

Status: **Phases 1-3 are built, and the parts of phase 4 that make sense here.** Real-network DHT, PEX and LSD are deliberately not built (see Phase 4).

## Where the brief and the real protocol differ

The brief was written against an assumed API; the repo's differs in these places, and the plugin follows the repo.

| Brief | Repo | Consequence |
|---|---|---|
| `"exec": "tracker-sim.exe"`, `"permissions": ["net","fs"]` | `exec` is an array; `permissions` is `{net:[...], fs:[...]}` | manifest uses `["tracker-sim.exe"]` and `{"net":["*"],"fs":[]}`. Windows-only `exec`, as for the other first-party plugins. |
| Settings type `list` with `options` (a dropdown) | `list` is a repeating-rows field; there is no dropdown | enum settings are `text` with the allowed values in the help text. Unknown values fall back to the default with a warning. |
| `torrents_updated` "notification or request" | always a notification, never answered | handled as a notification; answered only if an `id` is present. |
| `compact_peers=1` | the real parameter is `compact=1` | `compact=1` is sent. |
| Torrent has `torrent_file_path` | not sent today | not used; the plugin needs only `info_hash`, `total_size`, `trackers`. |
| Core change needed | `simulator` kind is already in core | no core change, and no allow-list fallback. |
| Settings for DHT, PEX, proxy, piece strategy | nothing implements them in Phase 1 | left out of the form until their phase (a setting that does nothing misleads). Listed per phase below. |

No Drift core file was changed for the plugin itself.

## Phase 1 behaviour

- **Nothing runs by default.** `torrents_updated` only fills a registry. A job starts when `selection_changed(enabled=true)` arrives, or when a torrent that was on before a restart shows up in the list.
- **Per torrent.** Job = N peer tasks (`peer_count`, default 1). Disabling one torrent signals only its job; each peer sends `event=stopped` (`numwant=0`) and saves its counters. A torrent that disappears from Drift's list stops too, but its switch is kept.
- **Quick replies.** The main thread only reads stdin and updates state; announces run on two tokio workers. `selection_changed` returns after spawning. `shutdown` stops all jobs and waits up to 5 s for their `stopped` announces.
- **Waiting for details.** A magnet without metadata has no size or trackers; its peers wait (5 s polls) rather than announce nonsense.
- **Trackers.** HTTP(S) only; every http tracker in the torrent is announced to in turn. `udp://` trackers are skipped (BEP 15 is not implemented).

### Modes

| Mode | Phase 1 | Counters |
|---|---|---|
| `seed_only` | yes | `left=0`, `downloaded` stays 0, `uploaded` grows |
| `partial_seed` | synthetic | starts with `partial_availability_percent` of the size, downloads the rest, uploads in proportion to what it owns |
| `download_and_seed` | synthetic | starts at 0, `left` shrinks at the download rate; upload scales with the owned fraction; `event=completed` once when `left` reaches 0 |
| `loopback_pair` | yes (Phase 2) | real: counted from bytes that crossed a socket; needs 2+ peers (raised to 2 when switched on) |

Selection (mode, peer count) is fixed when the torrent is switched on, from the settings at that moment, and saved; changing the defaults later affects only torrents switched on afterwards.

### Identity (`identity.rs`, `profiles.rs`)

Created once, saved in `state.json`, shared by all torrents (like one client): `identity_id` (`bot-001`...), `peer_id` (20 chars: client prefix + random body in that client's style), `key`, `port`, `client_profile`, `personality_seed`. `auto` picks qBittorrent/Transmission/Deluge by seed (50/30/20 %). The profile is kept per identity; changing the setting later does not change an existing peer. `debug_obvious` uses `-DRSIM0-` so the bots are easy to find in tracker logs; no other profile contains an obvious marker. The profile also sets User-Agent, `numwant` and port style. Peer number *i* of any torrent is identity *i*.

Personality (from the seed, so it survives restarts): rate scale, idle and burst probability, announce-interval factor. `stealth_level` scales jitter and how far personalities spread, and sets the chance of a stretched gap (basic 0, medium 2 %, high 5 %, maximum 8 %).

### Announce schedule (`peer.rs`)

Per peer, independent: a random first delay (5-100 % of `start_delay_max_ms`), then the time to the next announce:

- **Following the tracker** (`follow_tracker`, on by default): the tracker's `interval` (clamped to 10 s – 1 h), or its `min interval` if that is larger, plus a little: `interval × (1 + jitter × random × 0.5)`. Never earlier than asked, as a real client's timer would behave. About 5 % of gaps (at `high`) are stretched 1.5–3×.
- **Otherwise** (no interval in the reply, or the option off): `base × peer habit × (1 ± jitter)`, sometimes ×1.5–3. Minimum 1 s.

Failure: 2 s, 4 s, 8 s ... up to 5 min, ±20 %. A reply with a `failure reason` (a deliberate refusal) waits at least 60 s × the number of refusals in a row (up to 5 min). A completed event that failed is re-armed. No tracker known: retry in 10 s without counting a failure.

Event sequence: `started` (first success; optional), none for periodic, `completed` once, `stopped`. All values are cumulative. If the tracker sent a `tracker id`, it is echoed as `trackerid`. A `warning message` is logged once per distinct text.

### Tracker data (`demand.rs`, `follow_tracker`)

The announce reply (and the scrape, when enabled) carries `complete` and `incomplete`: seeders and leechers on the torrent. They include our own peers, which are counted per torrent and subtracted. The rest is demand:

- **Upload** (announce-only modes): no other leechers → the upload band is scaled to 0 (a seeder with nobody to serve uploads nothing). Otherwise the factor is `leechers / (other seeders + 1)`, between 0.05 and 1. It scales the configured min and max, so the speed profile's shape is kept.
- **Download** (modes that download): no other seeders → stalled. 1 seeder → about 0.33, 3 or more → full.
- Counts missing from a reply leave the speeds unchanged. In `loopback_pair` the speeds are real transfers and the tracker's numbers do not change them (the interval still applies).
- Everything from the tracker is clamped; a huge number cannot make a peer misbehave.

Verified against `bittorrent-tracker`: two seeders alone on a torrent read seeders 0 and leechers 0 after subtraction, and uploaded stayed 0.

### Rate engine (`rate.rs`)

Tick 200 ms. Profiles: `constant_jitter` (target near the middle, ±15 %, smoothed), `random_walk` (mean-reverting walk inside min/max), `bursty` (phases normal / burst / idle with random lengths; idle drops to 0, so bursty can go below min). A token bucket turns the rate into bytes: capped at 2 s of max rate, and at 1.5× max per tick. Counters only advance after the first successful announce (a client uploads only once it has announced). Ponytail ceiling: counters pause while a request is in flight; a time step is clamped to 0.5 s so a slow tracker can't cause a catch-up lump.

### State (`state.rs`)

`<data_dir or ../.tracker-sim>/state.json` (the plugin folder is the working directory, so by default the file sits beside the plugin folders in `<data>/plugins/.tracker-sim/`, where replacing or updating the plugin folder doesn't touch it; Drift ignores a folder without a `plugin.json`): `{version, identities[], torrents{hash:{enabled,mode,peer_count,started_at}}, counters{"bot-001:<hash>":{uploaded_bytes,downloaded_bytes,have_bytes?,last_announce_time}}}`. Written to `state.json.tmp` then renamed. A corrupt file is renamed `state.json.corrupt` and the plugin starts empty. Counters are saved after every successful announce and on stop. `persist_counters=false` starts from zero each run (the peer still has the same identity).

### Tracker client (`tracker.rs`)

`GET <announce>?info_hash&peer_id&port&uploaded&downloaded&left&key[&event]&numwant&compact=1&no_peer_id=1&supportcrypto=1[&apikey=...][&extra_query]`. `info_hash` is percent-encoded raw bytes by default; `info_hash_encoding=hex` for trackers that want text. Existing query parts of the announce URL (passkeys) are preserved. API key: secret field `tracker_api_key`, fetched with `get_secret` inside `initialize` (never from a notification), sent as the parameter named by `api_key_param`. The response is parsed as bencode for `failure reason` (a failure) and `interval`. Errors and logs never print the full URL (it may hold a passkey).

Scrape (`scrape_enabled`): `/announce` becomes `/scrape` (BEP 48), GET with the info hash, result ignored. Ponytail: no per-profile scrape pattern yet.

### Errors

Every request gets a valid JSON reply (`-32602` for a bad hash, `-32601` for an unknown method); unparseable lines and replies are skipped. Bad settings fall back to defaults. Tracker failures retry with backoff and never reach the RPC loop. Known limit: the workspace release profile uses `panic = "abort"`, so a panic in a task would end the process (Drift restarts it: 2 s, 4 s, then off on the third). The code avoids panicking paths; there is no `unwrap` on input.

## Stealth criteria, and how each is met

1. Only selected torrents: default-off, tested (`selection_is_per_torrent_and_survives_restart`, integration test).
2. Disabling one keeps others: per-torrent `Job`, tested.
3. Stable peer ids across restarts: identities in `state.json`, tested.
4-5. Non-periodic, desynchronised announces: per-peer RNG, personality factor, first delay; tested.
6. Gradual counters: rate engine + clamp; tested at unit level and over HTTP.
7. Counters persist; 8. no obvious prefix outside `debug_obvious`; 9. started/periodic/stopped events; 10. no host failures in normal use (replies are immediate).

What Phase 1 cannot hide: it sends no peer traffic, so a tracker (or a swarm crawler) that connects to the announced port finds nothing there; all peers of one machine share its IP; and there is no real data behind `uploaded`. Phases 2 and 3 address these.

## Per-torrent settings and statistics (Drift 0.1.28 and newer)

The plugin declares `torrent_settings` and `"torrent_status": true` in `plugin.json`, so Drift's right-click menu for a torrent offers:

- **Simulate this torrent** opens a dialog (mode, peers, upload and download speed pattern and limits, partial share, piece strategy, follow tracker data, daily activity pattern) before anything starts. The dialog starts from `get_options`: the torrent's own values if it is simulated, else the plugin's global settings.
- **Simulation settings…** (when on) changes them. A change restarts that torrent's peers (their `stopped` goes out first and counters carry on). The same values sent again change nothing.
- **Simulation statistics…** shows a Peers table (state, uploaded, downloaded, left, announces, next announce, last answer), what the tracker says (seeders and leechers other than ours, interval) and the last 40 events. Drift asks every 2 s while the window is open; the numbers live in the plugin's memory only (a few KB per torrent) and are dropped when the torrent is switched off or the plugin exits.

Per-torrent choices are saved with the torrent (`options` in `state.json`) and laid over the global settings (`Settings::for_torrent`): only the twelve per-torrent keys can be overridden, so identity-level settings (stealth level, client profile, proxies) stay global. Without a form (an older Drift) the plugin behaves as before: the global settings at the moment the torrent is switched on.

## Phase 2: real BitTorrent transfer (built for `loopback_pair`)

Modules `wire.rs` (BEP 3 handshake and messages), `pieces.rs` (bitfield and piece strategies), `swarm.rs` (nodes, sessions, choker, synthetic content).

- **Topology.** One swarm per torrent, built by the first peer to reach it once the size is known. Every peer listens on 127.0.0.1 (an ephemeral port; the port announced to the tracker is still the identity's, and nothing listens on it, see "What it cannot hide"). Peer 1 starts with every piece, the others with none; each later peer connects to all earlier ones, so leechers also trade with each other.
- **Wire.** Handshake, `bitfield`, `have`, `interested`/`not interested`, `choke`/`unchoke`, `request`/`piece`/`cancel`, keep-alive every 2 min. 16 KiB blocks, 16 requests in flight per connection. Frames over 4 MB, bad bitfields, requests outside a piece and a wrong info hash end the connection.
- **Data.** Synthetic: every byte is a function of (torrent, piece, offset), so no file is read and nothing is stored. A piece is accepted only if its SHA-256 equals the hash of what the generator produces for it (our own metainfo; real `.torrent` hashes would need `torrent_file_path`, which core does not send). A failed piece is discarded and counts for nothing.
- **Counters.** `uploaded` = payload bytes handed to the socket by that peer; `downloaded` = bytes of verified pieces; `left` = size minus verified bytes. `completed` is announced once, when the last piece verifies. Nothing moves before the peer's first successful announce. The configured upload/download rate profiles become the bandwidth limiters (a token bucket per direction per peer), so a transfer has the same shape as the announce-only counters.
- **Choking.** Every `unchoke_secs` (profile: Transmission 10 s, libtorrent clients 15 s), or at once when someone becomes interested: the 4 interested peers that moved the most data with us are unchoked, plus one optimistic unchoke re-drawn every 30 s. A choke drops the peer's pending requests.
- **Piece selection** (`piece_strategy`): `sequential`; `random`; `rarest_first` (random for the first 4 pieces, then the least available, random among ties; the default); `endgame_bias` (rarest first, and when every missing piece is already requested elsewhere, request it again from this peer and `cancel` the duplicates when one copy arrives).
- **Limits.** The swarm is for local validation: when the leecher has everything the transfer is over and counters stay put (the peers keep announcing). After a restart a leecher starts with as many whole pieces (from the front) as its saved `have_bytes`, so `left` and `downloaded` carry on instead of repeating the download; `uploaded`/`downloaded` continue from their saved totals. Which pieces it has is made up; only the amount is real. Not built: the fast extension, the extension protocol, real files, magnet metadata fetch (BEP 9), `partial_seed`/`download_and_seed` against a real swarm.

## Phase 3: network identity diversity (built for tracker requests)

`proxy.rs`. Settings: `proxy_mode` (`none`, `shared`, `per_peer`), a `proxies` list (address, optional user name, secret password via `get_secret("proxies.<row>.password")`) and `outbound_address`.

- `shared`: every peer uses the first proxy. `per_peer`: peer *N* uses proxy *N*. **A peer with no proxy of its own does not run**, rather than announce from the real address (it logs why).
- The assignment is saved in `state.json` (`proxy_id` on the identity), so an identity keeps its address between runs, even if the list is reordered; a removed proxy falls back to the slot by position.
- Each peer has its own HTTP client (`socks5h://` recommended so the tracker's name is resolved by the proxy; `socks5://`, `http://`, `https://` work). `outbound_address` binds a local IP when no proxy is used (picks the network card).
- Logs show a proxy's id, never its address or password. Errors carry no URL.
- **Not built:** peer-wire connections through a proxy. The loopback swarm is local, so it has none; real peer traffic would need SOCKS5 BIND or a VPN with a forwarded port. Running one Drift per container/VM with its own IP remains the sturdiest way to get distinct addresses.

Done when: two identities announce from two different source addresses in the tracker's log, and an identity keeps its address after a restart (the integration test checks routing and the saved assignment; distinct source addresses need real proxies).

## Phase 4: swarm realism

Built:
- **Client-specific announce shape.** Transmission profiles send the parameters in Transmission's order and without `no_peer_id`; libtorrent profiles (qBittorrent, Deluge) in libtorrent's. Per-profile unchoke timing (Phase 2).
- **Long-term behaviour** (`activity_pattern`, off by default, `activity.rs`): the rate band is scaled by a per-identity daily curve (peak mid-afternoon, 30 % at night, shifted per identity) and a smooth day-to-day drift of +-15 %, derived from the seed and the clock, so it is the same after a restart and needs no stored state. The configured min/max are for full activity.

Not built, on purpose:
- **DHT (BEP 5), PEX (BEP 11), LSD (BEP 14).** They matter only to peers on a real network swarm, and this plugin's peers never join one (nothing listens on the announced port). Doing them would also publish the torrent's info hash to the public DHT or the local network, which a tracker test doesn't need and the user didn't ask for. A tracker cannot see them, so they would not affect what it is being tested against. If a swarm-level test is ever wanted, build them on top of a listener on the announced port, behind `enable_dht`/`enable_pex`/`enable_lsd` settings that default off.
- **Client-specific scrape patterns.** Scrape is one GET per interval for every profile.

## Tests

- Unit (in the crate): settings validation, rate bounds and burst limits, state round-trip and corruption, identity stability, peer counters in each mode, announce jitter and backoff, URL building (both styles), bencode answers, selection and resume logic, RPC parsing; wire codec round-trips and refusals, bitfield and piece strategies, choker selection, proxy assignment, activity factor; and the loopback swarm over real TCP (three peers finishing with verified data, endgame, nothing moving before activation, corrupt data refused and not counted).
- Integration (`tests/protocol.rs`, real binary, stub HTTP tracker, ~30 s): initialize / `torrents_updated` / `get_selection` / `selection_changed` / `shutdown`; nothing announced before selection; `started` first with uploaded 0; cumulative, monotonically growing counters with no single announce carrying most of the total; uneven gaps; `stopped` with `numwant=0` on disable and on shutdown; peer id and counters continue after a restart; a per-peer proxy receives the announces (a stub plays the proxy) and a peer without one stays quiet; a loopback pair announces counters that match the bytes moved (leecher's `left` falls to 0 with `completed`, seeder's `uploaded` covers it).

## Manual test

1. Install: Settings → Plugins → online install, or by hand (quit Drift first) run `scriptspackage.ps1` and unzip `dist	racker-sim-<version>.zip` into `<Drift data folder>plugins	racker-sim` so `plugin.json` and `tracker-sim.exe` sit directly in it. The folder name equals the id.
2. Start Drift, Settings → Plugins → Tracker Simulator → turn on (check the permissions prompt). Suggested test settings: Announce interval `5000`, Longest first delay `3000`, Upload rate profile `random_walk`, Log level `debug`.
3. Run a tracker you control (any local one). A throwaway check without a real tracker: a local listener that logs requests (PowerShell: `[System.Net.HttpListener]` on `http://127.0.0.1:6969/`, answer body `d8:intervali1800e5:peers0:e`), and a test torrent whose announce URL is `http://127.0.0.1:6969/announce` (legal data only, e.g. a Linux ISO).
4. Add the torrent in Drift, right-click it → Simulator plugins → Tracker Simulator: the tick appears with "Simulating: seed_only, 1 peer".
5. Watch the tracker log for 60 s: first request has `event=started`, `uploaded=0`; later requests have growing `uploaded`, irregular gaps, no `event`. View log (Settings → Plugins) shows the `[DEBUG] announce ...` lines.
6. Untick: a request with `event=stopped&numwant=0`. A second torrent ticked meanwhile is unaffected.
7. Quit and restart Drift: the tick is still on; the first request has the same `peer_id` and `uploaded` ≥ the last value seen.
8. Loopback: set Default mode `loopback_pair` (and, to see it finish quickly, upload/download min and max 2000-4000 KB/s on a small torrent), switch a torrent on: two peers announce; one reports `left=0`, the other's `left` falls to 0 with `event=completed`. The log shows `loopback swarm started`.
9. Proxies: add a row under Proxies (for example `socks5h://127.0.0.1:1080` from a local SSH `-D` tunnel), set Proxy mode `per_peer`, and set Default peer count to 2: only peer 1 announces and the log says `bot-002 not started: ... no proxy`.
10. Automated equivalent: `cargo test`.
