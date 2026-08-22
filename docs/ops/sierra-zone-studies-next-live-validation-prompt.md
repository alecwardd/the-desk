# Next work block prompt — Sierra zone studies live validation and tightening

Paste the prompt below into the agent working in `C:\the-desk` during the next suitable market window.

---

You are working in `C:\the-desk`, but the two Sierra Chart studies under test live in `T:\SierraChart`.

## Objective

Finish evidence-based validation and tightening of:

- `T:\SierraChart\ACS_Source\VolSignalsZoneBoard.cpp`
- `T:\SierraChart\ACS_Source\TapePaceZones.cpp`

Do not treat the studies as trusted until each relevant acceptance criterion below is actually observed. Start diagnostically. Change code or settings only when the evidence identifies a specific problem.

## Read first

Read these current files in order:

1. `C:\the-desk\AGENTS.md`
2. `C:\the-desk\AGENT.md`
3. `C:\the-desk\CLAUDE.md`, especially the Never-Do list and RTH/Globex separation rule
4. `C:\the-desk\docs\agent-interaction-guide.md`
5. `C:\the-desk\docs\trader-memory\identity.md`
6. `C:\the-desk\skills\trading-domain\SKILL.md`
7. Owner-held private provenance, not present in this repository: 2026-08-06 Sierra zone studies session transcript, topic slug `sierra-zone-studies`.
8. Owner-held private provenance, not present in this repository: 2026-08-11 Sierra zone studies live visual check transcript, topic slug `sierra-zone-studies-live-visual-check`.
9. Both ACSIL source files and `T:\SierraChart\ACS_Source\BuildInkZoneStudies.bat`

Inspect `git status` in `C:\the-desk` before doing anything. The worktree already contains unrelated user changes; preserve them exactly. Do not stage, commit, push, clean, reset, or rewrite unrelated files.

## Known current state

- Both studies are loaded on the live `ESU6.CME/MESU6.CME 2000 Volume #4 Heikin-Ashi` chart.
- The visual pass succeeded. Alec especially likes the VolSignals presentation; preserve its hierarchy and styling unless a concrete defect is found.
- VolSignals displayed nine coherent zones, but its readout was `basis +23.05 manual`. That validates the manual fallback and conversion geometry only—not the live reference-chart path.
- The local `SPX_CGI.scid` file was stale at the August 11 check, and no live reference sample was established before the 3:00 PM CT SPX cash/update-window close.
- Tape Pace rendered in its lower region, but its trigger sigma/lookback have not been tuned against a bounded Globex observation.
- The safe VolSignals architecture is **Reference Chart Number**. Never reintroduce an active `$SPX` request or `sc.GetSymbolDataValue(..., true)` on the Rithmic feed.

## Hard safety boundaries

- Do not place, modify, cancel, flatten, or reverse any trade or order.
- Do not change account, broker, Rithmic, data-service, entitlement, login, or subscription settings.
- Avoid chart-trading controls entirely. Do not record or copy positions, account balances, P/L, order IDs, or raw live-market logs into tracked files.
- Do not restart Sierra, reconnect the feed, reload the whole chartbook, or run a disruptive full recalculation during an active session without Alec's explicit action-time approval.
- Do not use the study's `Symbol` basis mode unless the reference symbol is already safely subscribed; the target validation path is **Reference Chart Number**.
- If `SPX_CGI` is unavailable, stale, or unentitled, stop that phase and report the blocker. Do not work around it by changing services or purchasing access.
- Keep RTH and Globex evidence explicitly separate.
- No repository Git action unless separately authorized.

## Phase 1 — Preflight and connection safety

1. Confirm the current source and DLL timestamps:
   - `T:\SierraChart\Data\VolSignalsZoneBoard_64.dll`
   - `T:\SierraChart\Data\TapePaceZones_64.dll`
2. Confirm both studies are loaded on the intended test chart and record their current inputs before changing anything.
3. Confirm the VolSignals source still uses the non-subscribing reference path and contains no `GetSymbolDataValue(..., true)` call.
4. Confirm Sierra's ES data remains fresh for at least 60 seconds and the Message Log shows no repeated deny/reconnect loop attributable to the study.
5. If connection health is questionable, stop and return `BLOCKED_CONNECTION_HEALTH`; do not tune around a feed problem.

## Phase 2 — Live RTH SPX→ES basis validation

Run this before **3:00 PM America/Chicago** while SPX cash is moving.

1. Open or identify a fresh `SPX_CGI` chart without changing data-service entitlements. Record its Sierra chart number and verify that its last price changes at least twice.
2. In VolSignals, keep:
   - `Levels Are Quoted In = SPX Cash`
   - `SPX Price Source For Basis = Reference Chart Number`
   - the manual fallback enabled only as a fallback
3. Set `SPX Reference Chart Number` to the verified `SPX_CGI` chart number.
4. Observe for at least 60 seconds and require all of the following:
   - readout state becomes `live`, not `manual` or `held`;
   - ES and SPX timestamps are fresh;
   - three spot checks of `ES last - SPX last` agree with the displayed smoothed basis within 1.0 point, with any wider gap explained rather than waived;
   - the basis has the correct sign and plausible magnitude based on those same-time observations, not yesterday's configured fallback;
   - at least two rendered levels equal `SPX source level + displayed basis` within chart tick/label rounding;
   - ES market data remains fresh and no reconnect loop appears.
5. If the readout stays `manual`, the live-basis phase is a failure even if the zones look correct. Diagnose the reference chart/input path without changing broker or entitlement settings.

## Phase 3 — Cash-close and overnight hold validation

1. Record the final displayed `live` basis immediately before the 3:00 PM CT update-window close.
2. After the close, require the readout to change to `held`.
3. Observe an ES price change while SPX remains frozen and verify that:
   - the held basis remains unchanged at displayed precision;
   - converted zones do not drift with the frozen SPX print;
   - no new basis samples are taken outside the update window.
4. Continue or repeat the observation in Globex if needed.
5. Explicitly distinguish continuous-runtime hold behavior from restart/recalculation behavior. Audit whether removing/re-adding the study, a full recalculation, or a Sierra restart loses the held sample and falls back to manual. Do not perform a disruptive restart during active trading; inspect the code first and schedule a safe test if needed.
6. If overnight persistence across restart is not implemented, document that limitation and recommend whether it should remain a deliberate fallback behavior or be persisted safely. Do not silently claim the overnight requirement passed across restart.

## Phase 4 — Tape Pace Globex audit and tuning

Use a clearly labeled Globex-only observation window. Do not combine it with RTH evidence.

1. Record the current settings, including metric, baseline method, lookback, trigger sigma, continuation sigma, burst lengths, minimum pace, zone height, merge behavior, and reset behavior.
2. Before tuning, audit the code path for session contamination. Determine whether the rolling baseline immediately after the RTH/Globex boundary includes bars from the prior session bucket. `Reset Zones At Globex / RTH Boundary` is not sufficient if baseline samples still cross the boundary.
3. If RTH and Globex samples are mixed, treat that as a correctness defect. Patch the baseline to use only samples from the current session bucket before tuning thresholds. Keep behavior deterministic and incremental.
4. Observe a bounded 30–60 minute Globex window and report:
   - valid pace samples and visible z-score range;
   - candidate burst count and created-zone count per hour;
   - rejected bursts and why;
   - overlapping/merged zones;
   - tiny/noisy zones versus clearly meaningful acceleration zones;
   - calculation time and any visible chart lag.
5. Compare only a small number of candidate settings, changing one dimension at a time. The current defaults are Median/MAD, lookback 120, trigger 2.5, continue 1.0. Do not optimize to one attractive chart segment or invent an ideal zone frequency.
6. Prefer a recommendation plus evidence over immediate production changes. Preserve the visual style Alec already likes.

## Phase 5 — Tightening and rebuild, only if warranted

If a concrete defect or clearly superior bounded setting is established:

1. Limit edits to the two ACSIL source files and the existing build script only if required.
2. Create timestamped source backups before editing because `T:\SierraChart` is outside the Git worktree.
3. Keep the safe reference-chart architecture; never add an active unsupported-symbol subscription.
4. Build with `T:\SierraChart\ACS_Source\BuildInkZoneStudies.bat`.
5. Require a clean compile, verify DLL timestamps/hashes, reload only the affected study when safe, and inspect the Message Log for load/runtime errors.
6. Re-run the acceptance criterion affected by the change. A successful compile is not a live validation.
7. Do not alter `C:\the-desk` implementation files merely to mirror the external ACSIL source.

## Required outcome

Return a concise result with separate verdicts:

- `VolSignals visual/manual fallback`: PASS / FAIL
- `VolSignals live RTH basis`: PASS / PARTIAL / FAIL / BLOCKED
- `VolSignals post-close hold`: PASS / PARTIAL / FAIL / BLOCKED
- `VolSignals restart/recalculation behavior`: PASS / LIMITATION / NOT TESTED
- `Tape Pace session separation`: PASS / FAIL
- `Tape Pace Globex tuning`: PASS / PARTIAL / NOT ENOUGH EVIDENCE
- `Connection safety`: PASS / FAIL

Include:

- exact chart and study/input configuration used;
- evidence for each verdict and the observation window in CT and ET;
- any code/settings changed, with paths and before/after values;
- build/reload checks;
- remaining risks and exact next step.

Write the durable technical report to `C:\the-desk\docs\ops\sierra-zone-studies-validation-YYYY-MM-DD.md`. Keep raw screenshots, account state, order state, and raw logs out of tracked files. If the session is routed through the second-brain project page, perform the required privacy-safe vault after-session writeback there as well.

Stop rather than claiming success when the required market window, fresh `SPX_CGI` data, or observable evidence is unavailable.

---
