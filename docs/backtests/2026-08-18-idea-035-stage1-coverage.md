# IDEA-035 — Stage 1 Coverage Check (gap scan + RTH N)

**Date:** 2026-08-18 · **Mode:** RESEARCH (coverage gate only — no leg engine, no Stage 1 parameter study, no Stage 2, no signal claim)
**Spec:** [docs/setup-ideas/IDEA-035-leg-to-leg-profile-engine.md](../setup-ideas/IDEA-035-leg-to-leg-profile-engine.md)
**Run artifacts (outside git):** `T:\TheDesk\temp\backtests\idea-035-stage1-coverage\run-20260818T082457Z\` (`coverage_probe.json`, `coverage.db`, `summary.json`)

## Verdict

**PREREQ_MET — N=163 eligible RTH days overall; clean 2026-06-23+ fold N=36.**

The current NQ front-month `.scid` coverage is sufficient to start the separately
authorized IDEA-035 Stage 1 boundary-stability study. All 81 RTH 5-minute coverage
buckets retain N=163 after the quality gate, and all 81 retain N=36 in the clean
2026-06-23+ fold. Both are **Reportable** (`N >= 30`) under `AGENT.md`.

This is only the data-coverage prerequisite. It does **not** mean Stage 1 passed,
and it does not authorize the offline leg-engine replay binary, Stage 2,
`get_leg_profile`, signals, ACSIL, or live/trading changes.

## Method (repo-true semantics)

- **Coverage probe before N:** an isolated SCID coverage summary equivalent to
  `get_research_summary` was written to `coverage_probe.json` and the
  `coverage_probe` table before the quality-gated N matrix was calculated. It is
  scoped by `root_symbol=NQ`, explicit contract, and `session_type`.
- **Data:** the three explicit front-month files were opened read-only:
  `NQH6.CME.scid`, `NQM6.CME.scid`, and `NQU6.CME.scid` under
  `T:\SierraChart\Data\`. The scanner used the SCID layout from
  `src/feed/scid_reader.rs` (56-byte header, 40-byte records, Sierra epoch).
- **Campaign:** 2025-12-15 through the last completed RTH session on 2026-08-17.
  Established rollover evidence supplies H6 through 2026-03-13, M6 from
  2026-03-16 through 2026-06-12, and U6 from 2026-06-15 onward. Only the current
  U6 window was extended beyond the earlier 2026-07-21 coverage check.
- **Session:** NQ RTH only, half-open `[09:30, 16:15)` ET per `src/lib.rs`.
  Globex was scanned only to apply the repo-true whole-day quality gate; it is
  not pooled into RTH N.
- **Gap rule:** consecutive monotonic ticks in the same session class, with a
  material gap strictly greater than 5 minutes in RTH or 30 minutes in Globex
  (`src/backfill.rs`). IDEA-035's shorthand “backfill 30-min logic” is therefore
  incomplete for RTH; this check obeys the implementation and does not invent a
  third threshold.
- **Exclusions:** dates in `EXCLUDED_SESSION_DATES` were removed. Any date with
  a material RTH or Globex gap was conservatively excluded as a whole day. The
  RTH bucket matrix is also reported so that whole-day and bucket coverage are
  explicit.
- **Isolation:** all results were written only to the run directory's
  `coverage.db` and JSON summaries. The live `data.db`, feed configuration,
  MCP server paths, and `src/pipelines/` were not opened for write or changed.

## Coverage probe (recorded before N claims)

Raw observed-day counts below include holiday/partial sessions; the quality
matrix that follows applies calendar and gap exclusions.

| Root | Contract | Window | RTH observed days | Globex observed days |
|---|---|---|---:|---:|
| NQ | NQH6.CME | 2025-12-15 → 2026-03-13 | 63 | 63 |
| NQ | NQM6.CME | 2026-03-16 → 2026-06-12 | 64 | 65 |
| NQ | NQU6.CME | 2026-06-15 → 2026-08-17 | 46 | 46 |

No MNQ, ES, MES, or Globex observations are included in the RTH baseline.

## Findings

### RTH day coverage by front-month window

Expected counts are weekdays after the repo calendar exclusions.

| Contract | Window | Expected RTH days | Observed | Missing | Eligible after gap gate |
|---|---|---:|---:|---:|---:|
| NQH6.CME | 2025-12-15 → 2026-03-13 | 59 | 59 | 0 | 59 |
| NQM6.CME | 2026-03-16 → 2026-06-12 | 63 | 63 | 0 | 63 |
| NQU6.CME | 2026-06-15 → 2026-08-17 | 44 | 44 | 0 | 41 |
| **All windows** | 2025-12-15 → 2026-08-17 | **166** | **166** | **0** | **163** |

Seven observed partial/holiday RTH dates were excluded by the repo calendar:
2025-12-24, 2025-12-31, 2026-01-19, 2026-02-16, 2026-05-25,
2026-06-19, and 2026-07-03. Fully closed configured dates had no RTH row and
are already absent from expected N.

### Material gaps and degraded-day exclusions

| Trading day | Contract | Session class | Gap (ET) | Duration |
|---|---|---|---|---:|
| 2026-06-26 | NQU6.CME | Globex | 03:50:51 → 04:31:11 | 40.333 min |
| 2026-07-30 | NQU6.CME | RTH | 11:48:59 → 11:55:23 | 6.400 min |
| 2026-08-06 | NQU6.CME | RTH | 10:36:23 → 10:41:25 | 5.019 min |
| 2026-08-06 | NQU6.CME | RTH | 10:41:27 → 10:56:59 | 15.533 min |

The gate excludes three whole days: **2026-06-26, 2026-07-30, and
2026-08-06**. The two August 6 gaps count as one degraded day. The June gap is
outside RTH, but whole-day exclusion is intentionally conservative and matches
the prior coverage method.

Non-monotonic records were dropped before gap measurement: H6 349, M6 5,888,
U6 1,566. This follows the replay guard and is reported as source quality, not
silently treated as eligible ticks.

### RTH coverage matrix and reliability

| Fold | RTH buckets | Min N | Median N | Max N | N>=30 buckets | Tier |
|---|---:|---:|---:|---:|---:|---|
| Full campaign, quality-gated | 81 | **163** | 163 | 163 | 81/81 | Reportable |
| Clean subset 2026-06-23+, quality-gated | 81 | **36** | 36 | 36 | 81/81 | Reportable |

The clean fold contains 39 expected non-holiday RTH days and excludes the three
degraded days above, leaving N=36. Thus the IDEA-035 prerequisite of at least
30 RTH days per reported coverage bucket is met both overall and on the clean
forward-recorded subset.

## Caveats and boundary

- N here is **covered RTH days**, not detected legs, anchors, or parameter-cell
  outcomes. Event/leg N remains unknown until a separately authorized Stage 1
  replay exists.
- The coverage gate cannot establish boundary stability, chop/trend separation,
  provisional-anchor lead time, or profile-volume accounting. Those are Stage 1
  study outputs.
- No statistics here are an edge or signal claim. No raw ticks, positions,
  balances, broker data, or proprietary signal definitions were copied into
  this note.

## Next gate

With separate authorization, build and run the offline IDEA-035 leg-engine
replay against this isolated, quality-gated RTH day set and the locked
`k × p × N × bin-size` grid. Stage 2 remains locked unless that later study
documents a Stage 1 pass and receives trader confirmation.
