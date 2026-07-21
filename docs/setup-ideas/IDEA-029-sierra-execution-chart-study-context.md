---
id: IDEA-029
title: Sierra Execution Chart Study Context and Exports
status: Researched
regime: [any]
related: [IDEA-006, IDEA-019, IDEA-020, IDEA-022, IDEA-024, IDEA-028]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/sierra-chart-settings.md
  - docs/mcp/tool-reference.md
mcpPointers:
  - tool: get_rvol
    note: Current repo-native participation pace for advisory volume-bar sizing research
  - tool: get_tape_pace
    note: Repo-native tape speed/volume context to compare against visual tape studies
  - tool: get_delta_profile
    note: Session/segment delta profile context already exposed to agents
  - tool: get_footprint
    note: Current volume-at-price, bid/ask, and delta context by price zone
  - tool: get_footprint_window
    note: Historical time-windowed footprint for leg/profile reconstruction research
  - tool: get_imbalances
    note: Existing stacked/diagonal imbalance context for tape/profile comparisons
  - tool: query_signal_outcome_distribution
    setupId: IDEA-029
    note: Future backtest outcome query; do not quote stats from this file
hypothesisAnchor: false
---

# IDEA-029 - Sierra Execution Chart Study Context and Exports

> Docs-only research note captured 2026-07-09 from the copied vault dispatch
> summary. No inbox notes, screenshots, Sierra chartbooks, private data paths, or
> live chart settings were opened or changed.

## Thesis

Sierra execution-chart studies can improve the trader's screen and the agents'
market narration, but they should not become trade signals by import. The Desk
should treat them as one of three things:

- **Repo-native context:** deterministic fields The Desk can compute from
  `.scid` / `.depth` data and expose with freshness, scope, and sample metadata.
- **Advisory chart context:** human-facing Sierra settings or visual studies that
  may inform a manual read, but do not alter live chart settings or fire alerts.
- **Offline research input:** exported study values/settings that can be aligned
  to timestamps and backtested before any agent-facing promotion.

The smallest safe design is a research queue, not a runtime integration:
document the candidate context fields, compare them with current MCP tools, and
only later add deterministic pipelines/tools for fields that survive backtests.

## Source Limits

The vault task referenced untracked inbox notes and a screenshot, but this run
did not access vault raw inbox files or attachments. The only source details from
those notes used here are the copied dispatch summary:

- Adaptive volume-bar sizing: NQ 250 and ES 500 are today's fixed chart sizes;
  the captured formula targets bar duration using expected volume/minute times
  RVOL. This is advisory-first and must never auto-change the live chart.
- Tape Reader / Order Flow Labs: review ideal settings, smoothing, whether a
  spreadsheet-study export should feed agent context in real time, and compare
  the Delta Dynamics equivalent.
- Leg-to-leg volume/delta profile: prior analysis says The Desk already has
  delta/footprint ingredients; the missing piece is a deterministic swing-leg
  boundary model plus a future `get_leg_profile` MCP tool.
- Sierra spreadsheet-study export: evaluate whether indicator values, firing
  history, and settings ranges can safely feed agent context/backtests and
  settings suggestions.

## Public References Checked

- Sierra Chart custom chart bar ACSIL docs:
  <https://www.sierrachart.com/index.php?page=doc/ACSIL_CustomChartBars.html>
- Sierra Chart spreadsheet study export / special tasks:
  <https://www.sierrachart.com/index.php?page=doc/SpreadsheetStudiesSpecialTasks.php>
- Sierra Chart spreadsheet study inputs:
  <https://www.sierrachart.com/index.php?page=doc/SpreadsheetStudyInputs.html>
- OrderFlow Labs Vertical Recon Tape:
  <https://orderflowlabs.com/pages/vertical-recon-tape>
- OrderFlow Labs Leg 2 Leg Profiles:
  <https://orderflowlabs.com/pages/leg-2-leg-profiles>
- OrderFlow Labs VBP:
  <https://orderflowlabs.com/pages/ofl-vbp>
- Delta Dynamics:
  <https://deltady.com/>
- Delta Dynamics products:
  <https://deltady.com/products>

Repo evidence checked:

- `docs/setup-ideas-and-backtesting.md` already has IDEA-019 for adaptive
  session-pace volume bars and ties it to Sierra ACSIL custom chart bars.
- `docs/decision-log.md` records prior rejection of Sierra spreadsheet export as
  a primary live feed and ACSIL push plugins as a maintenance-heavy route.
- `docs/mcp/tool-reference.md` already exposes `get_rvol`, `get_tape_pace`,
  `get_delta_profile`, `get_footprint`, `get_footprint_window`, and
  `get_imbalances`.
- `skills/mcp-tools/SKILL.md` reinforces the live-vs-historical split and says
  research must begin with coverage/sample checks.

## Cross-Cutting Recommendation

Keep Sierra execution-chart studies trader-facing until The Desk can reproduce
or ingest their outputs as structured, timestamped, scope-labeled data.

Agents may say:

> "This chart study is context. It supports/weakens the read if it lines up with
> NQ structure, flow, and playbook rules, but it is not a trigger."

Agents must not say:

> "The Tape Reader / leg profile / exported indicator fired, so take the trade."

For runtime context, prefer existing Desk-native MCP fields over vendor UI
states. For research, export values only into an offline, timestamp-aligned file
or database table with clear provenance and no private account/config fields.

## Track A - Adaptive Volume-Bar Sizing

### Captured Idea

Today's fixed Sierra execution-chart volume bars are NQ 250 and ES 500. The
research idea is to size bars by target duration:

```text
contracts_per_bar =
  expected_volume_per_minute(symbol, session_time_bucket)
  * current_rvol_multiplier
  * target_bar_seconds / 60
```

Then round and clamp:

```text
advisory_size =
  round_to_step(clamp(contracts_per_bar, min_size, max_size), step_size)
```

This can be useful because "250 NQ contracts" means something different during
RTH open, lunch, MOC, and slow Globex. A duration-targeted bar attempts to keep
execution-chart information density stable without hiding participation changes.

### Recommendation

Do not auto-change Sierra chart settings. Treat the formula as an advisory
calculator first:

- Keep NQ 250 and ES 500 as the observed fixed baselines until backtests show a
  reason to change them.
- Compute advisory alternatives from Desk-native expected volume pace and RVOL.
- Display the result as "suggested bar size for research/manual review", not as a
  runtime command.
- Use session buckets and keep RTH/Globex separate; never mix them into one
  baseline.
- Add smoothing only to reduce jumpiness in advisory values, not to suppress
  real pace spikes.

Safe agent context:

- current fixed chart size as human-supplied context,
- current expected size band from RTH/Globex-specific pace,
- current RVOL/tape-pace classification,
- bar-duration distribution from offline replay.

Advisory/offline only:

- modifying Sierra chart bar size,
- ACSIL custom chart bar implementation,
- automatic settings mutation,
- treating an adaptive bar setting as a setup trigger.

### Backtest Questions

- Does adaptive duration targeting improve setup outcome distributions versus
  fixed NQ 250 / ES 500 volume bars and time bars?
- Does it stabilize bars-per-RTH-segment without hiding meaningful pace
  acceleration at the open, macro windows, and MOC?
- Which target durations and clamps preserve order-flow readability for NQ and
  ES separately?
- Does smoothing reduce settings churn without adding enough lag to miss the
  opening speed regime?
- Do existing setups such as OR5 Mid Retest, rebid/reoffer, absorption failure,
  and pinch retain or improve signal quality when replayed on adaptive bars?

## Track B - Tape Reader / Order Flow Labs / Delta Dynamics

### Captured Idea

Review ideal Tape Reader settings, smoothing, and whether a Sierra
spreadsheet-study export should feed agent context in real time. Compare the
Order Flow Labs tape-style read with Delta Dynamics equivalents.

Public OrderFlow Labs material describes Vertical Recon Tape as a Time & Sales
visualization that turns prints into size/color bubbles and highlights sweeps.
Public Delta Dynamics material lists Sierra/MotiveWave order-flow studies,
including an Orderflow Pack with Absorption, Aggression, Confluence, VFlip, Tape,
and Divergence, plus Delta Bars.

### Recommendation

Use vendor tape studies for screen design and manual calibration, not as an
agent signal source.

- Prefer The Desk's `get_tape_pace`, `get_delta_profile`, `get_footprint`,
  `get_imbalances`, and `get_absorption_events` as canonical agent context.
- If vendor study values can be exported with timestamps, use them as offline
  labels/features for comparison against Desk-native metrics.
- Settings review should catalogue candidate parameters such as minimum print
  size, sweep definition, lookback/smoothing window, and visibility thresholds.
  Do not store private screenshots or chartbook config in tracked docs.
- Smoothing should be evaluated as a lag/noise tradeoff. A smoothed tape field is
  context only unless backtests show it adds incremental value.

Safe agent context:

- Desk-native tape pace, volume/sec, ticks/sec, acceleration, data coverage, and
  stale/live metadata,
- Desk-native footprint/delta/imbalance/absorption summaries around a level,
- offline-exported vendor states after timestamp alignment and provenance checks.

Advisory/offline only:

- vendor visual state when methodology is opaque,
- settings screenshots,
- real-time spreadsheet polling from an active Sierra sheet,
- a Tape Reader or Delta Dynamics state as a standalone entry/exit trigger.

### Backtest Questions

- Which vendor tape states, if exportable, map cleanly to Desk-native pace,
  delta, footprint, and absorption metrics?
- Does sweep/aggression context improve existing setup outcomes after controlling
  for price location, RVOL, day type, and key-level proximity?
- What smoothing windows minimize false positives without delaying absorption or
  exhaustion reads?
- Do Order Flow Labs and Delta Dynamics identify the same events, or are their
  settings/definitions too different for a shared context field?
- Does adding tape-study context improve agent explanations, or does it duplicate
  data already captured by `get_tape_pace` and footprint tools?

## Track C - Leg-to-Leg Volume/Delta Profile

### Captured Idea

Build swing-anchored per-leg volume/delta profiles. Prior analysis says The Desk
already has delta and footprint primitives; the missing abstraction is a
deterministic swing-leg boundary model plus a future `get_leg_profile` MCP tool.

Public OrderFlow Labs material frames Leg 2 Leg Profiles as rotational profiles
anchored to swing highs/lows, resetting as a new rotation forms. OFL VBP also
describes rotational/leg-to-leg, session, time-anchored, and delta-capable volume
profiles.

### Recommendation

Do not copy a vendor chart study. Recreate the useful abstraction in repo-native
terms:

1. Define a deterministic swing/rotation boundary model. Candidate inputs:
   minimum price reversal, minimum elapsed time, minimum traded volume, and
   session-scope rules.
2. Accumulate volume-at-price and delta-at-price within the active leg using the
   same tick/footprint primitives The Desk already trusts.
3. Expose a compact future `get_leg_profile` context object: leg direction,
   anchor time/price, age, volume, net delta, POC/HVN/LVN, value area, delta POC,
   and confluence with session profile levels.
4. Keep the output as context until backtests show which leg-profile events have
   repeatable outcomes.

Safe agent context:

- current leg direction/age/volume/delta,
- leg POC/HVN/LVN and whether they overlap session profile levels,
- stale/insufficient labels when the leg boundary model is unstable or too new.

Advisory/offline only:

- vendor rotation sensitivity settings,
- chart overlay appearance,
- unbacktested "LVN retest" or "delta POC shift" claims,
- any profile event that lacks sample size and session-scope labels.

### Backtest Questions

- Which swing boundary rule creates stable legs without over-segmenting chop?
- Do current-leg LVN/HVN retests improve rebid/reoffer, absorption, or OR5
  continuation outcomes?
- Does leg delta profile add information beyond existing session delta profile
  and footprint windows?
- Does confluence between leg LVN/HVN and full-session LVN/HVN predict rejection,
  continuation, or no reliable difference?
- What sample size is needed per day type and session segment before agents can
  frame leg-profile reads as meaningful context?

## Track D - Sierra Spreadsheet-Study Indicator/Settings Export

### Captured Idea

Evaluate whether Sierra spreadsheet-study indicator values, firing history, and
settings ranges can safely feed agent context, backtests, and settings
suggestions.

Sierra's spreadsheet studies can output chart and study data to spreadsheet
columns and can save tab-delimited text files. The docs also warn that row count
affects CPU cost and that multiple studies writing to the same spreadsheet/sheet
can create conflicts.

### Recommendation

Use spreadsheet-study export for offline research first. Keep it out of the live
agent loop until there is a deliberately designed ingest boundary.

Offline use is reasonable when:

- the export has explicit symbol, chart/study name, timestamp, bar/session scope,
  and settings provenance,
- files are copied into a repo-safe research-import location without account or
  chartbook secrets,
- exports are immutable for a given research run,
- backtests compare exported states against Desk-native event/outcome tables.

Live use requires a later design:

- isolated read-only export directory,
- atomic file writes or append-only records,
- schema version,
- freshness/staleness checks,
- duplicate-row and recalculation guards,
- throttle/backpressure behavior,
- no writes back into Sierra.

Safe agent context:

- offline imported study values with timestamps and provenance,
- settings ranges as candidate research metadata,
- firing history only after alignment to `.scid` time and session scope.

Advisory/offline only:

- active spreadsheet polling from Sierra's Data folder,
- private chartbook/settings screenshots,
- automatic settings suggestions during live trading,
- any export that lacks stable timestamps or can be affected by spreadsheet
  recalculation artifacts.

### Backtest Questions

- Do exported indicator firing events align to Desk-native event timestamps
  within an acceptable tolerance?
- Which settings ranges are robust across RTH sessions, Globex segments, and day
  types?
- Does an exported study state improve outcome prediction after controlling for
  existing Desk-native features?
- How often do spreadsheet recalculation, duplicate sheet names, row limits, or
  CPU load produce missing/stale/duplicated records?
- Can settings suggestions be produced from historical evidence without implying
  they should change the live chart automatically?

## Safe Context Matrix

| Candidate input | Safe for agent context now? | Safe route |
|-----------------|-----------------------------|------------|
| Desk-native RVOL/tape/delta/footprint fields | Yes | Existing MCP tools with freshness and scope labels |
| NQ 250 / ES 500 fixed chart-size note | Yes, as human-supplied context | Manual note only; no chart mutation |
| Adaptive volume-bar advisory size | Later, after offline replay | Derived context field, not a Sierra command |
| Vendor Tape Reader / Delta Dynamics visual state | No live feed yet | Offline labeled research only |
| Leg-to-leg profile levels | Later | Future deterministic swing-leg model and `get_leg_profile` |
| Sierra spreadsheet-study values | Offline only now | Timestamped export/import with provenance |
| Sierra spreadsheet-study live polling | No | Needs separate design and failure-mode review |
| Settings suggestions | Advisory only | Backtest-derived notes; human applies or rejects |

## Failure Modes to Design For

- Missing or stale exports: agent reports "no fresh chart-study context" and falls
  back to repo-native fields.
- Ambiguous session scope: block or downgrade the context; never combine RTH and
  Globex silently.
- Duplicate spreadsheet sheets or recalculation artifacts: reject the import run
  rather than inventing a clean series.
- Vendor methodology mismatch: label as vendor-specific and do not merge into a
  shared field without backtest evidence.
- Over-smoothing: record the lag/noise tradeoff explicitly, especially around RTH
  open and fast liquidation/short-covering moves.
- Chart-setting drift: tracked docs should record candidate settings; they should
  not claim to know the live chart unless Alec supplies that state.

## Recommended Sequencing

1. Keep NQ 250 / ES 500 as baseline chart context and run an offline adaptive-bar
   replay spec against existing setup outcomes.
2. Catalogue candidate Tape Reader and Delta Dynamics settings from Alec-supplied
   summaries only; do not store private screenshots/config in the repo.
3. Prototype the leg boundary model on historical `.scid`/footprint data before
   adding any `get_leg_profile` MCP surface.
4. Pilot one spreadsheet-study export as an offline import with schema,
   timestamp, symbol, session, and settings provenance.
5. Promote only fields that add incremental value over existing Desk-native tools
   and clear the repo's sample-size policy.

## See Also

- Hub stub: [setup-ideas-and-backtesting.md#idea-029](../setup-ideas-and-backtesting.md#idea-029)
- Existing adaptive bar body: [setup-ideas-and-backtesting.md#idea-019](../setup-ideas-and-backtesting.md#idea-019)
- Setup index: [index.md](index.md)
