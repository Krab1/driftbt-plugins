//! Tracker Simulator: a Drift `simulator` plugin that, for the torrents the user switches on, announces to their
//! trackers as stable, human-looking peers with gradually growing counters. For testing trackers you operate.
//! Protocol: the plugin protocol (PLUGIN_PROTOCOL.md in the Drift repo). Design and phases: docs/tracker-sim-design.md.

#[macro_use]
mod log;
mod activity;
mod demand;
mod identity;
mod jobs;
mod peer;
mod pieces;
mod profiles;
mod proxy;
mod rate;
mod rng;
mod rpc;
mod settings;
mod state;
mod swarm;
mod tracker;
mod wire;

use std::collections::VecDeque;
use std::io::BufRead;

use serde_json::{json, Value};

use jobs::{Sim, Torrent};
use settings::Settings;

fn main() {
    // Two workers: the main thread stays on stdin and never does network work.
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("a runtime always builds");
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    let mut later: VecDeque<String> = VecDeque::new();
    let mut sim: Option<Sim> = None;

    while let Some(line) = later.pop_front().or_else(|| rpc::next_line(&mut lines)) {
        let msg = match rpc::parse_line(&line) {
            Ok(m) => m,
            Err(e) => {
                debug!("skipping a line: {e}");
                continue;
            }
        };
        let id = msg.id.clone().unwrap_or(Value::Null);
        let reply = match msg.method.as_str() {
            "initialize" => {
                let mut settings = Settings::from_json(&msg.params["settings"]);
                log::set_level(&settings.log_level);
                if sim.is_none() {
                    let api_key = rpc::ask_secret(&mut lines, &mut later, "tracker_api_key");
                    for i in 0..settings.proxies.len() {
                        let key = format!("proxies.{}.password", settings.proxies[i].id);
                        settings.proxies[i].password = rpc::ask_secret(&mut lines, &mut later, &key);
                    }
                    info!("tracker-sim initialized stealth_level={:?} client_profile={}", settings.stealth, settings.client_profile);
                    sim = Some(Sim::new(settings, &api_key, rt.handle().clone()));
                }
                json!({"kinds": ["simulator"], "name": "Tracker Simulator", "version": env!("CARGO_PKG_VERSION")})
            }
            "torrents_updated" => {
                let list = msg.params["torrents"].as_array().into_iter().flatten().filter_map(|t| serde_json::from_value::<Torrent>(t.clone()).ok()).collect();
                sim.get_or_insert_with(|| Sim::new(Settings::default(), "", rt.handle().clone())).torrents_updated(list);
                json!({})
            }
            "get_selection" => json!({"selections": sim.get_or_insert_with(|| Sim::new(Settings::default(), "", rt.handle().clone())).selections()}),
            "selection_changed" => {
                let hash = msg.params["info_hash"].as_str().unwrap_or("");
                let enabled = msg.params["enabled"].as_bool().unwrap_or(false);
                let torrent = serde_json::from_value::<Torrent>(msg.params["torrent"].clone()).ok();
                let s = sim.get_or_insert_with(|| Sim::new(Settings::default(), "", rt.handle().clone()));
                if let Err(e) = s.selection_changed(hash, enabled, torrent) {
                    if msg.id.is_some() {
                        rpc::send(&rpc::err(&id, -32602, &e));
                    }
                    continue;
                }
                json!({})
            }
            "shutdown" => {
                if let Some(mut s) = sim.take() {
                    s.shutdown();
                }
                if msg.id.is_some() {
                    rpc::send(&rpc::ok(&id, json!({})));
                }
                return;
            }
            other => {
                debug!("unknown method {other}");
                if msg.id.is_some() {
                    rpc::send(&rpc::err(&id, -32601, "method not found"));
                }
                continue;
            }
        };
        // A notification (torrents_updated) is never answered.
        if msg.id.is_some() {
            rpc::send(&rpc::ok(&id, reply));
        }
    }
    // Drift closed our stdin without `shutdown`: still stop the peers politely.
    if let Some(mut s) = sim.take() {
        s.shutdown();
    }
}
