---
name: unknowns-pass
description: Map-vs-territory blindspot checklist. USE WHEN starting a substantial change — new pipelines or condition fields, MCP tool surface changes, setup templates, backtests in a new area, or any work in an unfamiliar part of the repo.
---

# /unknowns-pass

A pre-implementation blindspot pass. The map (prompt, spec, docs, your assumptions) is
never the territory (the code, the data, the constraints). Walk the four quadrants below
*before* writing code or registering a hypothesis. Every item in §4 is a failure mode this
repo has already paid for once — the goal is to never pay for the same one twice.

Skip this for trivial edits. Run it whenever the change is substantial, touches an area
you have not worked in before, or crosses a layer boundary.

## 1. Known knowns — is the map current?

Read the nearest living doc for the area, then spot-check one of its claims against source:

| Area | Canonical map | Territory to spot-check |
|------|---------------|-------------------------|
| Pipelines / market math | `skills/trading-domain/SKILL.md` | `src/pipelines/` |
| Rules / condition fields | `CLAUDE.md` + `src/rules/mod.rs` doc comments | `ConditionField`, `RULES_ENGINE_SCHEMA_VERSION` |
| Backtesting / recorded data | `docs/data-and-backtesting-guide.md` | `src/backfill.rs`, `src/research/` |
| MCP tool surface | `docs/mcp/README.md` (checklist) | `src/bin/the-desk-mcp/tools/`, generated `docs/mcp/tool-reference.md` |
| Setup ideas | `docs/setup-ideas/index.md` (catalog + verdicts, one file per idea) | `src/rules/setup_templates.rs` |

If the map and the territory disagree, fix or flag the map as part of the change — do not
silently work around it.

## 2. Known unknowns — what is already open?

- Any **Pending ADR** in `docs/decision-log.md` touching this area? Do not implement past
  an undecided ADR — resolve its "open items" with the trader first.
- Any relevant idea status in the `docs/setup-ideas/` catalog? Call
  `list_hypotheses` and skim the verdicts — **never re-test a rejected idea** in its
  rejected form (IDEA-000 and IDEA-012 were rejected as specified but reconstructed in
  IDEA-020; know which form is settled).

## 3. Unknown knowns — what would Alec flag on sight?

- Trading semantics and doctrine: `docs/trader-memory/identity.md`,
  `playbook-doctrine.md`, `lessons-learned.md`.
- Engineering conventions: `CLAUDE.md` (Never-Do list, terminology), `AGENT.md`
  (Common Mistakes, Grounded Partnership).
- If an unwritten-preference question would change the design, **ask one question at a
  time, highest architectural impact first** (see AGENT.md "Map vs Territory
  Conventions"). Do not batch ten questions or guess on the architecture-changing one.

## 4. Unknown unknowns — walk the known failure modes

Check every line that applies to the change:

- [ ] **Layer placement.** Which of Layer 1 / 2 / 2.5 / 3 does this touch? Market math,
      conditions, and data processing go in Rust; synthesis goes in agents. No LLM calls
      from Rust, ever.
- [ ] **Scope.** Live vs historical data path correct? RTH vs Globex explicitly scoped —
      never mixed without labeling?
- [ ] **Incremental math preserved.** No full-profile recomputation per tick.
- [ ] **Rules-engine change?** Any `ConditionField` / operator / evaluate-semantics
      change requires: bump `RULES_ENGINE_SCHEMA_VERSION`, rebuild the release binary
      (`target_alt`), restart the MCP server. A stale binary silently rejects new fields.
- [ ] **Contract scope.** Backtest window matches the front contract — pass `contract`
      to `run_backtest`; treat `scid_window_mismatch_warning` / `integrity_status:
      "warning"` as a stop sign, not noise.
- [ ] **Logical consistency of conditions.** Can the gate and the entry co-occur at all?
      (IDEA-000 v2: regime required price *away* from VWAP while the entry required price
      *at* VWAP — N=2 after a full backtest cycle. Ten minutes of checking would have
      caught it.)
- [ ] **Sample feasibility first.** `register_hypothesis({ dryRun: true })` →
      check `feasibleForN30` / `projectedSampleSize` before spending a run.
- [ ] **Over-firing.** Is any condition a re-evaluated *state flag*? Set
      `duplicateSuppressionMs` so one event is one signal; watch the `chatty` /
      `over_firing` flags in `summarize_hypothesis_run`.
- [ ] **Tool surface change?** Regenerate docs (`cargo run --bin the-desk-mcp --
      --write-tool-docs`) and keep the three drift guards green
      (`cargo test --bin the-desk-mcp`).
- [ ] **Golden replay impact.** Will `tests/session_replay_golden.rs` drift? Bless only
      after deciding the drift is intended domain behavior, never to make CI pass.
- [ ] **Single-source docs.** No new duplicated rule surfaces — pointers only (the
      AGENTS.md find/replace corruption is why).

## Output

End the pass with a short note (in the conversation, or as an implementation note per
AGENT.md if the work is substantial):

1. Assumptions you are proceeding on.
2. Map/territory mismatches found and what you did about them.
3. Questions for the trader — only ones whose answer changes the design.
4. Which §4 items applied and their status.

If nothing surfaced, say so in one line and proceed. The pass is a gate, not a report.
