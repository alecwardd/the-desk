# 2026-07-22 Top-10 Offline Backtest Campaign

**Campaign ID:** `2026-07-22-top-10-campaign`
**Development end (inclusive):** `2026-07-21`
**Recorded:** `2026-07-22T20:17:00-05:00` (America/Chicago)
**Overall status:** `mixed_completed_and_blocked` — 2 completed development studies; 8 parked behind definition/data blockers; **no live activation**.

---

## Executive summary (outcome-first)

Two studies finished as honest **DEVELOPMENT_RESULT / PARK_FORWARD_VALIDATION** development runs with isolated DBs and no live config/risk/playbook mutation:

1. **#1 Unified IDEA-011 + IDEA-025B (IB campaign V2)** — primary matched gate **underperformed** the ungated baseline (`gross ΔAvgR = −0.1818`, weekly bootstrap CI `[-0.75, 0.0]`, `matched_n=11`). **Must not be promoted** to live or acceptance.
2. **#8 IDEA-025D 9AM hour continuation** — promising **directional-overlay / event-probability** evidence (green/red NY continuation both ≈66.7% with positive lift vs unconditional bases; N reportable). **Not an entry system.** Remains **PARK_FORWARD_VALIDATION**; an **untouched holdout after 2026-07-21** is required before any future acceptance consideration. ACCEPT is structurally impossible for this study.

The other eight studies were **honestly parked** without fabricated results: they need locked predicates and/or accrued data before a first executable campaign run.

---

## Scope and status taxonomy

**Scope:** Top-10 ranked offline research ideas executed or parked during the 2026-07-22 Cursor/Codex campaign window, using local Sierra `.scid` data and isolated campaign DBs under `T:\TheDesk\temp\backtests\`.

**Fixed status values (records):**

| Status | Meaning |
|--------|---------|
| `completed_development` | Full isolated replay produced artifacts; verdict is development/park only — never live ACCEPT. |
| `blocked_definition` | Cannot execute until predicates/contracts are locked. |
| `blocked_data` | Definition sufficient enough to know what data is missing; accrual/gates required. |
| `blocked_data_and_definition` | Both definition locks and data accrual required. |

Campaign decision outcomes for completed studies: `DEVELOPMENT_RESULT` + `PARK_FORWARD_VALIDATION` only. **No live activation.**

---

## All ten studies

| # | IDEA ID / name | Status | Evidence / result | Decision | Exact unblock condition |
|---|----------------|--------|-------------------|----------|-------------------------|
| 1 | IDEA-011 + IDEA-025B — Unified IB (one-sided 30m acceptance vs ungated baseline @ 1R) | `completed_development` | Canonical V2 run; primary gate underperformed; `matched_n=11` | PARK; **do not promote** | Future untouched matched holdout **after 2026-07-21** only if revisiting; still not a promotion of this discovery result |
| 2 | IDEA-020 — Footprint lifecycle | `blocked_data` | No campaign run | Parked | Clean post-**2026-06-23** / live-eye event accrual to **N≥30**; do **not** retune exhausted NQH6 history |
| 3 | IDEA-014 — London unwind | `blocked_definition` | No campaign run | Parked | Lock **trended materially**, reclaim, and handoff-state predicates |
| 4 | IDEA-025A — AM TBR | `blocked_definition` | No campaign run | Parked | Lock prior-session **percent-net-change** scope (RTH, Globex, or custom TBR) |
| 5 | IDEA-001 — Open Drive | `blocked_definition` | No campaign run | Parked | Lock OpenDrive decision clock/retrace; VWAP entry/fill; risk/exit; RVOL gate/freeze; primary matched control |
| 6 | IDEA-031 — Compression / expansion | `blocked_definition` | No campaign run | Parked | Lock percentile/threshold predicates and outcome/trade contract |
| 7 | IDEA-033 — Stage A (RV / volatility) | `blocked_definition` | No campaign run | Parked | Lock RV bar size, annualization, RV→points mapping, naive median lookback, interval method, walk-forward folds (**or** explicitly authorize ATR-only Stage A-prime and lock its remaining choices) |
| 8 | IDEA-025D — 9AM hour continuation | `completed_development` | Canonical Stage-2 review-fix run; directional overlay evidence | PARK_FORWARD_VALIDATION; not an entry system | Untouched holdout **after 2026-07-21**; keep ACCEPT structurally impossible |
| 9 | IDEA-003 — Naked VPOC | `blocked_definition` | No campaign run | Parked | Lock TPO POC vs true VPOC; fill predicate/tolerance; RTH vs Globex horizon; duplicate origin policy; roll carry/censor |
| 10 | IDEA-004 — CVD divergence | `blocked_data_and_definition` | No campaign run | Parked | Define true MTF CVD (1m/5m/15m) object/extreme/divergence predicates **and** accrue data; **do not** substitute absorption `delta_divergence` |

---

## Verified result — #1 Unified IDEA-011 + IDEA-025B

Parsed from current canonical artifacts (not chat prose).

### Canonical paths

- **Run dir:** `T:\TheDesk\temp\backtests\ib-campaign-2026-07-22\v2\run-20260722T175513Z`
- **Isolated DB:** `...\run-20260722T175513Z\ib-campaign.db`
- **Code paths:** `C:\the-desk\src\research\ib_campaign.rs`, `C:\the-desk\src\bin\the-desk-ib-campaign.rs`
- **Release binary:** `C:\the-desk\target\release\the-desk-ib-campaign.exe`
  sha256=`7bf25fcf765447b2362c3a0681a401507cc8823b4ccb32f68debcf1f086c4666`
- **Source hashes (provenance):**
  - `the-desk-ib-campaign.rs` sha256=`50e5c121ce802bb2a1eedfc939bc56449ad13228da3a35d9656a612e4a51fef4`
  - `ib_campaign.rs` sha256=`b00715af17e8c25286e5f8fef6181bcd8aa8cba4376cbbca8113c550be18522f`

### Artifact hashes (computed at record time)

| File | SHA256 |
|------|--------|
| `report.json` | `e69d40b9fee2218cc7e9e5df00dc1c64b00b75290eb392e2889f02a25eeb4757` |
| `REPORT.md` | `f9c9636e825e287a489ba72094437204f229a50da112656b251695e1bd45fd8b` |

### Population / primary metrics (`report.json`)

- Descriptive all: populationSessions=**154**, usableSessions=**119**, excludedSessions=**35**
- Descriptive discovery usableSessions=**119** (populationSessions=**154**)
- Primary contrast: treated=`one_sided_acceptance_30m` vs control=`ungated_baseline` @ **1R**
- **matched_n=11**
- Gross scenario: treatedAvgR≈**0.0909**, controlAvgR≈**0.2727**, **deltaAvgR≈−0.1818**
- Cost scenarios (labeled, not actual fees): 0.5pt and 1pt round-trip retain ΔAvgR≈−0.1818
- Weekly block bootstrap: nBlocks=7, replicates=1000, pointMeanDeltaR≈−0.1818, **CI95=[−0.75, 0.0]**
- Contract block bootstrap: nBlocks=2, CI95≈[−0.2222, 0.0] with sparse-block limitation noted in artifact
- **Verdict:** `DEVELOPMENT_RESULT` / `PARK_FORWARD_VALIDATION` (`futureHoldoutRequired=true`)

### Explicit promotion ban

The preregistered primary gate **underperformed** the matched ungated baseline on development data (negative ΔAvgR; weekly CI does not show a positive edge). **Do not promote** this result to live trading, playbook activation, or ACCEPT.

### Audit predecessor (noncanonical)

- `T:\TheDesk\temp\backtests\ib-campaign-2026-07-22\v2\run-20260722T174033Z` — pre-bootstrap correction; retained for audit only.
  `report.json` sha256=`d282bb32b966659665fc3bf33d229439376f50df8ae63f1eb9286f0220e87e85`
  `REPORT.md` sha256=`409c06a3219fd4cad03be80458327ce1a90c23fc1c40e2ce8c6eb3f48e5822e1`

---

## Verified result — #8 IDEA-025D 9AM hour continuation

Parsed from current canonical artifacts (review-fix run).

### Canonical paths

- **Run dir:** `T:\TheDesk\temp\backtests\nine-am-continuation-campaign-2026-07-22\v1\run-stage2-reviewfix-20260722-192008`
- **Isolated DB:** `...\run-stage2-reviewfix-20260722-192008\nine-am-continuation.db`
- **Code paths:** `C:\the-desk\src\research\nine_am_continuation.rs`, `C:\the-desk\src\bin\the-desk-nine-am-continuation.rs`
- **Release binary:** `C:\the-desk\target\release\the-desk-nine-am-continuation.exe`
  sha256=`85cc74e07a4e2c8f43a179dbc35451400ca55b0acf9c1ef71509f8af60cc6931`
- **Source hashes (provenance):**
  - `the-desk-nine-am-continuation.rs` sha256=`efd2e23607f8db997f016709d6a0fcb19d1304cb9732ecd740694610377376f6`
  - `nine_am_continuation.rs` sha256=`a658d1540614bf295db4bf5917fa0c9f35d26e03818fb59b0264655a9f5e321c`

### Artifact hashes (computed at record time)

| File | SHA256 |
|------|--------|
| `report.json` | `6aeb8ff7380eb9c5d153ed533b14271ce51ccf80b3e3e50d475734ae32613a54` |
| `REPORT.md` | `35283e55b09b20b13fd04465cb513c0c290ab0026686b21156bf1c1e8033fb5a` |

### Population / primary metrics (`report.json`)

- populationSessions=**155**, primary usable=**132**, primaryExcluded=**23**
- Green NY primary: N=**72**, wins=**48**, losses=**24**, rate=**0.6667**, Wilson95≈**[0.5518, 0.7647]**, unconditional base≈**0.5152**, lift≈**+0.1515**, tier=`reportable`
- Red NY primary: N=**60**, wins=**40**, losses=**20**, rate=**0.6667**, Wilson95≈**[0.5406, 0.7727]**, unconditional base≈**0.4848**, lift≈**+0.1818**, tier=`reportable`
- Pooled NY (secondary): N=**132**, rate=**0.6667**, Wilson95≈**[0.5825, 0.7414]**
- Full-session secondary: green N=72 rate≈0.5972; red N=59 rate≈0.5593; fullSessionDenominator=**131**
- Desk 16:15 sensitivity (own bases): green/red rates match primary in this sample (N=72/60)
- Excursion diagnostic: n=**132** (= primary usable), eligibility=`primary_usable_only: quality.usable (fatal_ok ∧ signal ∧ ny_primary)`
- **Verdict:** `DEVELOPMENT_RESULT` / `PARK_FORWARD_VALIDATION`

### Explicit interpretation

This is **promising directional-overlay / event-probability evidence**, **not** an entry/stop/target trade system (`primaryIsTradeSystem=false`, `acceptStructurallyImpossible=true`). It remains **PARK_FORWARD_VALIDATION**. Any future acceptance consideration requires an **untouched holdout beginning after 2026-07-21**.

### Audit predecessor (noncanonical)

- `T:\TheDesk\temp\backtests\nine-am-continuation-campaign-2026-07-22\v1\run-stage2-20260722-190807` — pre-report-eligibility correction (excursion/exclusion-count defect); retained for audit only.
  `report.json` sha256=`4250512e91e93146fc4fcc64aaa029fba88160437c7d798f7c010c2d40a6390e`
  `REPORT.md` sha256=`c4032de3d693ab8ca7b1fce58d1f8f668ede38fa17bb661883ff8e27a7bc5e58`

---

## Blocker decision packet (#2–#7, #9, #10)

Preserve the smallest known decisions; do not invent results.

### #2 IDEA-020 Footprint lifecycle — `blocked_data`

- Accrue clean post-**2026-06-23** / live-eye lifecycle events to **N≥30**.
- Do **not** retune against exhausted NQH6 history.

### #3 IDEA-014 London unwind — `blocked_definition`

- Lock predicates: **trended materially**, reclaim, and handoff-state.

### #4 IDEA-025A AM TBR — `blocked_definition`

- Lock prior-session **percent-net-change** scope: RTH, Globex, or custom TBR window.

### #5 IDEA-001 Open Drive — `blocked_definition`

- Lock: OpenDrive decision clock / retrace predicate; VWAP entry/fill; risk/exit; RVOL gate/freeze; primary matched control.

### #6 IDEA-031 Compression / expansion — `blocked_definition`

- Lock percentile/threshold predicates and the outcome/trade contract.

### #7 IDEA-033 Stage A — `blocked_definition`

- Lock: RV bar size; annualization; RV-to-points mapping; naive median lookback; interval method; walk-forward folds.
- **Or** explicitly authorize ATR-only Stage A-prime and lock that variant’s remaining choices.

### #9 IDEA-003 Naked VPOC — `blocked_definition`

- Lock: TPO POC vs true VPOC; fill predicate/tolerance; RTH vs Globex horizon; duplicate origin policy; roll carry/censor.

### #10 IDEA-004 CVD divergence — `blocked_data_and_definition`

- Define true MTF CVD (**1m / 5m / 15m**) object, extreme, and divergence predicates.
- Accrue supporting data.
- Do **not** substitute absorption `delta_divergence`.

---

## Provenance / safety

| Item | Value |
|------|-------|
| Cursor model | `cursor-grok-4.5-high` |
| Implementer chat | `8c77ae4f-8473-472c-89b7-b8c10474863b` |
| Planner chat | `69e1daf5-1b3f-40ca-b87a-0443b526a0b4` |
| Wrapper skill (machine path) | `C:\Users\alecw\.codex\skills\cursor-cli-orchestrator\SKILL.md` |
| Git commit recorded in runs | `5ecf648aba7a310a2c79f392a86916a42f194308` (dirty worktree during runs) |
| Development end | `2026-07-21` |

**Safety:** Campaign binaries reject live DB paths and used per-run isolated DBs only. No live config, risk controls, or playbooks were modified for these records. No git commit/push for this documentation request beyond creating these new files. Prompts, secrets, and auth data are **not** recorded here.

### Canonical vs audit predecessors

| Role | Path |
|------|------|
| Canonical IB (#1) | `...\ib-campaign-2026-07-22\v2\run-20260722T175513Z` |
| Audit IB predecessor | `...\ib-campaign-2026-07-22\v2\run-20260722T174033Z` |
| Canonical 9AM (#8) | `...\nine-am-continuation-campaign-2026-07-22\v1\run-stage2-reviewfix-20260722-192008` |
| Audit 9AM predecessor | `...\nine-am-continuation-campaign-2026-07-22\v1\run-stage2-20260722-190807` |

---

## Current repo code paths (completed studies)

| Study | Library | Binary |
|-------|---------|--------|
| #1 IB unified | `src/research/ib_campaign.rs` | `src/bin/the-desk-ib-campaign.rs` |
| #8 9AM continuation | `src/research/nine_am_continuation.rs` | `src/bin/the-desk-nine-am-continuation.rs` |

---

## Revisit checklist (resume order)

1. **#8 IDEA-025D** — after an untouched post-2026-07-21 holdout window exists; keep overlay-only framing; never ACCEPT from development alone.
2. **#2 IDEA-020** — once clean post-2026-06-23 events reach N≥30 (no NQH6 retune).
3. **#3 IDEA-014** — immediately after trended/reclaim/handoff predicates are locked.
4. **#4 IDEA-025A** — immediately after percent-net-change scope is locked.
5. **#5 IDEA-001** — immediately after OpenDrive clock/VWAP/risk/RVOL/matched-control locks.
6. **#6 IDEA-031** — immediately after percentile/threshold + trade contract locks.
7. **#9 IDEA-003** — immediately after VPOC/fill/horizon/duplicate/roll locks.
8. **#7 IDEA-033** — after RV (or authorized ATR A-prime) method locks.
9. **#10 IDEA-004** — after MTF CVD definition **and** data accrual (no absorption substitute).
10. **#1 IB unified** — only as a disciplined holdout revisit; discovery underperformance is not a promotion candidate.
