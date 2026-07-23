# Backtest Campaign Archive

This directory is the durable **index and decision record** for offline backtest campaigns run for The Desk.

## Contract

- **Run artifacts stay outside git** (typically under `T:\TheDesk\temp\backtests\...`). They are not committed.
- Files here are the **human/machine decision index**, not a substitute for on-disk artifact hashes. Always re-verify `report.json` / `REPORT.md` SHA256 against the absolute run path before acting on a result.
- Campaign binaries used isolated DBs under each run directory and refuse live DB paths. Records here must not embed secrets, prompts, auth tokens, or live config/risk/playbook contents.

## Campaigns

| Campaign | Record |
|----------|--------|
| 2026-07-22 Top-10 offline campaign | [2026-07-22-top-10-campaign.md](./2026-07-22-top-10-campaign.md) · [2026-07-22-top-10-campaign.json](./2026-07-22-top-10-campaign.json) |
