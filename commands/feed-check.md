---
name: feed-check
description: Verify Sierra SCID / optional .depth feed health. USE WHEN troubleshooting live data, path resolution, or after feed-ingestion changes.
---

# /feed-check

1. Confirm `~/.the-desk/config.toml` has `sierra_data_dir` pointing at Sierra `Data` and a resolvable `.scid` for the active contract.
2. Run `cargo test` at the repository root (includes SCID reader + MCP helpers).
3. If MCP is in use, call `get_feed_health` and `validate_data_integrity` for `liveDataSource`, tail reset counts, and freshness metadata.

Live ingestion is **file-based only** (`.scid` + optional `.depth`); there is no in-repo TCP DTC client.
