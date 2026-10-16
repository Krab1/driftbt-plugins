# Drift plugins

Plugins for [Drift](https://github.com/Krab1/driftbt) that live outside the app. Each is a small separate program
that Drift starts and talks to over JSON lines (see `docs/PLUGIN_PROTOCOL.md` in the Drift repo).

| Plugin | Kind | What it does |
|---|---|---|
| [`tracker-sim`](crates/tracker-sim) | simulator | For torrents you switch on, announces to their trackers as stable, human-looking peers (optionally moving real data between its own peers over loopback). **For testing a tracker you operate or have permission to test.** Design: [docs/tracker-sim-design.md](docs/tracker-sim-design.md). |

## Install

In Drift: Settings → Plugins → turn on "Look for plugins and updates online" → Check → Install, then enable it (you'll see
its permissions first). Or by hand: unzip a release into `<Drift data folder>\plugins\<id>\` so that `plugin.json` is
directly inside it.

## Build and release

```powershell
cargo test                      # unit and integration tests
scripts\package.ps1             # release build -> dist\tracker-sim-<version>.zip, prints the sha256
```

To publish: bump `version` in the crate's `Cargo.toml` and `plugin.json` together, run the script, attach the zip to a GitHub
release here (tag `tracker-sim-v<version>`), then add the version (URL, sha256) to `registry/index.json` in the Drift
repo and sign it, as described in Drift's `docs/PLUGINS.md` ("Registry and updates").
