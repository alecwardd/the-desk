# Trader Memory

This directory is the hand-curated memory layer for The Desk. It captures durable trader identity, rules of engagement, and lessons that should travel across sessions.

SQLite remains the source of truth for trades, signal outcomes, behavioral patterns, and generated statistics. Auto-generated markdown digests live under `~/.the-desk/memory/` and should not be edited by hand.

Use these files for durable doctrine:

- `identity.md` — who the trader is, account constraints, recurring behavioral guardrails
- `playbook-doctrine.md` — setup-family rules of engagement and known false positives
- `lessons-learned.md` — dated lessons, postmortems, and rules promoted from experience
