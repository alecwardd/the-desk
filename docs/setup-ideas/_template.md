# New Idea Template

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

Every new IDEA entry uses this skeleton. The **Unknowns** and **Consistency check**
sections exist because their absence has already cost full backtest cycles (IDEA-000 v2's
regime gate required price accepted *away* from VWAP while its entry required price *at*
VWAP — N=2 after a full run; ten minutes on paper would have caught it). Fill them in
*before* registering a hypothesis, and run `commands/unknowns-pass.md` for anything
touching new pipeline detection or condition fields.

```markdown
### IDEA-NNN: <Name>

**Status:** Idea
**Source:** <doctrine session / local DB study / literature — with dates>
**Complements / supersedes:** <related IDEA entries and templates>

**Concept:** <2–4 sentences: the mechanic and why it should exist>

**Tweakable decisions (lead with these):**
| Knob | Starting value | Why / what would change it |
|------|----------------|-----------------------------|
| <threshold, window, ratio…> | <value> | <rationale; mark provisional values as provisional> |

**Setup mechanics:** <context / entry / stop / target — anchored to structure
(zones, levels), not free-floating flags; direction explicit>

**Known unknowns & invalidation criteria:**
- <what is assumed but unverified; what result kills the idea>
- <which regime/data window is a fair test, and which is not (e.g. the gappy
  NQH6 backfill vs live-recorded data — see IDEA-020 Stage 1.5 conclusion)>

**Instrumentation dependencies:**
- <fields/events the backtest needs; verified `signal_outcomes` only
  (`includeUnverified:false`); pipeline detection that must exist first;
  `RULES_ENGINE_SCHEMA_VERSION` bump if condition fields change>

**Consistency check:** <can every gate condition and the entry trigger co-occur?
Walk one concrete session through the conditions on paper before registering.>

**Settled-verdict check:** <adjacent rejected ideas from this doc + `list_hypotheses`;
what makes this version different from the rejected form>

**Backtesting hypotheses:**
> <falsifiable question with explicit measurement window>
```

Create the idea as `idea-NNN-<slug>.md` in this directory, add a row to the
catalog table in [index.md](index.md), and keep the `<!-- hypothesis-anchor: IDEA-NNN -->`
comment above the content if a hypothesis will reference it.
