# IDEA-034 — Stage 1 Coverage Check (gap scan + per-bucket N)

**Date:** 2026-07-24 · **Mode:** RESEARCH (coverage gate only — no analysis binary, no Stage 2, no signal claims)
**Spec:** [docs/setup-ideas/IDEA-034-time-of-day-liquidity-events.md](../setup-ideas/IDEA-034-time-of-day-liquidity-events.md)
**Run artifacts (outside git):** `T:\TheDesk\temp\backtests\idea-034-stage1-coverage-2026-07-24\` (`scan.py`, `coverage.db`, `summary.json`)

> **2026-08-11 status:** the later bucket-stats replay is now accepted after a
> clean-`b63e83a` provenance verification. Its gate-applied counts supersede
> this preliminary scan where they differ. See the
> [verification record](2026-08-11-idea-034-bucket-stats-verification.md).

## Verdict

**N ≥ 30 is reachable — YES, for every in-scope bucket.** All 267 in-scope 5-min ET buckets
retain N ≈ 145–147 eligible days after the gap/quality gate and holiday exclusions. The only
underpowered element is the clean-subset robustness fold (2026-06-23+, N ≈ 20 — directional
tier only), which needs more accrual time, not more gating.

## Method (repo-true semantics)

- **Data:** `T:\SierraChart\Data\NQ{H6,M6,U6}.CME.scid` replayed read-only (56-byte header, 40-byte records, SC-epoch µs — `src/feed/scid_reader.rs`).
- **Campaign window:** 2025-12-15 → 2026-07-21 (`CAMPAIGN_H6_START_DATE` / `DEVELOPMENT_END_DATE`).
- **Contract windows:** reused from ib-campaign v2 `rollover_evidence.json` (run-20260722T175513Z): H6 → 2026-03-13, M6 2026-03-16 → 06-12, U6 2026-06-15 → 07-21. File first/last ticks align exactly with window edges; no overlap or seam days.
- **Sessions/buckets:** ET clock, RTH [09:30, 16:15), Globex ≥ 18:00 or < 09:30, London ≥ 02:00 (`src/lib.rs`). 288 5-min buckets; 21 buckets in 16:15–18:00 are out of scope per repo session classification → 267 scored buckets. Trading day = ET date + 1 when time ≥ 18:00.
- **Gap scan (quality gate):** backfill.rs logic — consecutive same-session-class ticks, gap > 30 min (Globex) / > 5 min (RTH); non-monotonic ticks dropped (349 H6 / 5,888 M6 / 850 U6).
- **Exclusions:** `EXCLUDED_SESSION_DATES` holidays/early-closes (11 dates; 8 had data, excluded from eligible N); DST-mismatch weeks 2026-03-09 → 03-27 flagged as separate cohort, never silently pooled.
- **Isolation:** results written only to the run-dir `coverage.db`. Live `data.db` and all live/trading paths untouched.

## Findings

**Day coverage:** 147 expected weekday days (campaign window minus holidays) → **147 observed, 0 missing**. Per-day tick counts: min 215k (2025-12-26), median ~463k–513k per contract, max 973k. No tiny-tick days.

**Gap scan:** exactly **1 material gap** in 7 months: **2026-06-26 03:50:51 → 04:31:11 ET (40.3 min, Globex/London segment, NQU6)** → that day excluded by the gate (conservatively whole-day; bucket-level it only touches 03:55–04:25 buckets). Holiday early-close days show an exact 13:00→18:00 ET halt, but it bridges the RTH→Globex class boundary so it is (correctly) not a same-class gap — and those days are holiday-excluded anyway.

**Per-bucket N (eligible, gate-applied days):**

| Scope | Buckets | Min N | Median N | N≥30 | N 20–29 | N<20 |
|---|---|---|---|---|---|---|
| All in-scope | 267 | 145 | 146 | **267** | 0 | 0 |
| Asia (18:00–02:00) | 96 | 145 | 146 | 96 | 0 | 0 |
| London (02:00–09:30) | 90 | 145 | 146 | 90 | 0 | 0 |
| RTH (09:30–16:15) | 81 | 146 | 146 | 81 | 0 | 0 |
| Excl. DST-mismatch weeks | 267 | 131 | 132 | 267 | 0 | 0 |
| Clean subset 2026-06-23+ | 267 | 19 | 20 | **0** | 258 | 9 |

Thin spots (still N ≥ 145): overnight buckets 05:20 and 08:35 and the 18:00–18:30 open block each miss exactly one day.

## Caveats / open questions

- **Clean-subset robustness fold is underpowered (N ≈ 20, "directional" tier).** IDEA-034 Stage 1 step 6 (re-run on 2026-06-23+ data) cannot reach reportable N until roughly late September 2026. Stage 1 can pass on the full window with the fold reported as directional-only, or wait.
- **Documented "gappy, double-distribution NQH6" concern (hub ~lines 778–791) did NOT reproduce as timestamp holes** in this scan — the file is structurally complete at the 30-min/5-min gap level. That caveat appears to concern volume-distribution characteristics of the backfilled H6 era, which this coverage check does not measure; it remains a Stage-1-replay (bucket-stats) question.
- N here is **days of coverage per bucket**, not detected events — event N depends on Stage 1 detection rates and cannot be known before the bucket-stats replay exists.
- This pass made no anomaly, calendar, continuation/reversal, or edge claim, and ran nothing against the live DB.

## Next step (updated 2026-08-11)

Coverage and bucket-stats evidence gates are **cleared**. The next research
gate is separately authorized calendar/event-rate extraction against the
pre-registered windows. Stage 1 has not passed, and Stage 2 remains locked.
