# Setup Ideas — Index

Per-entity catalog of setup ideas and cross-cutting research tracks. Each row links to a
per-IDEA detail file. The hub, [setup-ideas-and-backtesting.md](../setup-ideas-and-backtesting.md),
keeps cross-cutting material (March 2026 snapshot, backtest results table, roadmap, queue) plus a
one-line stub anchor per IDEA pointing back here.

- **Detail lives here**, in `IDEA-NNN-*.md`.
- **Numbers do not.** Detail files carry `mcpPointers`; pull fresh stats from MCP/SQLite at query time.
- **Template:** [`_template.md`](_template.md). **Lint:** `cargo test docs_lint`.

> **Migration status (Phase 1b in progress):** `✅` = extracted to its own file · `⬜` = still in the hub, not yet migrated.

| ID | Title | File | Migrated |
|----|-------|------|:--------:|
| IDEA-000 | Regime-Gated Setup Selector | [IDEA-000-regime-gated-selector.md](IDEA-000-regime-gated-selector.md) | ⬜ |
| IDEA-001 | Opening Drive Classification | [IDEA-001-opening-drive-classification.md](IDEA-001-opening-drive-classification.md) | ⬜ |
| IDEA-002 | Trapped Trader Reversal | [IDEA-002-trapped-trader-reversal.md](IDEA-002-trapped-trader-reversal.md) | ⬜ |
| IDEA-003 | Naked VPOC Magnet Trade | [IDEA-003-naked-vpoc-magnet.md](IDEA-003-naked-vpoc-magnet.md) | ⬜ |
| IDEA-004 | Multi-Timeframe CVD Divergence | [IDEA-004-mtf-cvd-divergence.md](IDEA-004-mtf-cvd-divergence.md) | ⬜ |
| IDEA-005 | Session Transition Sweep Patterns | [IDEA-005-session-transition-sweep.md](IDEA-005-session-transition-sweep.md) | ⬜ |
| IDEA-006 | Volume Imbalance Bars (Lopez de Prado) | [IDEA-006-volume-imbalance-bars.md](IDEA-006-volume-imbalance-bars.md) | ⬜ |
| IDEA-007 | Microstructure Regime Detection | [IDEA-007-microstructure-regime-detection.md](IDEA-007-microstructure-regime-detection.md) | ⬜ |
| IDEA-008 | 0DTE Gamma Regime Trading | [IDEA-008-0dte-gamma-regime.md](IDEA-008-0dte-gamma-regime.md) | ⬜ |
| IDEA-009 | NQ/ES SMT Divergence | [IDEA-009-nq-es-smt-divergence.md](IDEA-009-nq-es-smt-divergence.md) | ⬜ |
| IDEA-010 | Fair Value Gap with Order Flow Confirmation | [IDEA-010-fvg-orderflow-confirmation.md](IDEA-010-fvg-orderflow-confirmation.md) | ⬜ |
| IDEA-011 | One-Sided IB Extension Acceptance | [IDEA-011-one-sided-ib-extension-acceptance.md](IDEA-011-one-sided-ib-extension-acceptance.md) | ⬜ |
| IDEA-012 | Absorption Failure / Liquidity Vacuum | [IDEA-012-absorption-failure.md](IDEA-012-absorption-failure.md) | ⬜ |
| IDEA-013 | Gamma-Gated Setup Overlay | [IDEA-013-gamma-gated-setup-overlay.md](IDEA-013-gamma-gated-setup-overlay.md) | ⬜ |
| IDEA-014 | London Inventory Unwind Into RTH | [IDEA-014-london-inventory-unwind.md](IDEA-014-london-inventory-unwind.md) | ⬜ |
| IDEA-015 | Post-Macro / Post-Earnings Jump Repair-or-Go | [IDEA-015-post-macro-jump-repair-or-go.md](IDEA-015-post-macro-jump-repair-or-go.md) | ⬜ |
| IDEA-016 | VWAP Pipeline Enhancements (Dual Session + Anchored) | [IDEA-016-vwap-pipeline-enhancements.md](IDEA-016-vwap-pipeline-enhancements.md) | ⬜ |
| IDEA-017 | MCP Product Hardening — Playbook & Guidance as First-Class Data | [IDEA-017-mcp-product-hardening.md](IDEA-017-mcp-product-hardening.md) | ⬜ |
| IDEA-018 | Multi-Instrument Concurrent Tracking (NQ, MNQ, ES, MES) | [IDEA-018-multi-instrument-concurrent-tracking.md](IDEA-018-multi-instrument-concurrent-tracking.md) | ⬜ |
| IDEA-019 | Adaptive Session-Pace Volume Bars (Sierra Chart ACSIL) | [IDEA-019-adaptive-session-pace-volume-bars.md](IDEA-019-adaptive-session-pace-volume-bars.md) | ⬜ |
| IDEA-020 | Footprint Rebid/Reoffer Zone Lifecycle | [IDEA-020-footprint-rebid-reoffer-lifecycle.md](IDEA-020-footprint-rebid-reoffer-lifecycle.md) | ⬜ |
| IDEA-021 | Multi-Instrument Flow Architecture (NQ / MNQ / ES / MES) | [IDEA-021-multi-instrument-flow-architecture.md](IDEA-021-multi-instrument-flow-architecture.md) | ⬜ |
| IDEA-022 | Rally Offer Replenishment / Touch Offer Exhaustion | [IDEA-022-rally-offer-replenishment.md](IDEA-022-rally-offer-replenishment.md) | ⬜ |
| IDEA-023 | Social Intelligence & Continual Learning (X / Trusted Accounts) | [IDEA-023-social-intelligence-continual-learning.md](IDEA-023-social-intelligence-continual-learning.md) | ⬜ |
| IDEA-024 | Market-Maker Pressure Inference | [IDEA-024-market-maker-pressure-inference.md](IDEA-024-market-maker-pressure-inference.md) | ✅ |
| IDEA-025 | NQStats Statistical Setup Library | [IDEA-025-nqstats-stat-library-setups.md](IDEA-025-nqstats-stat-library-setups.md) | ✅ |
