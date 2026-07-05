# Scratchpad — Chartbook MGI, Level Verification, and Microstructure Review

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Idea (working notes — not a spec for immediate implementation)

This section captures prior chartbook / strategy framing and a checklist of **Market Generated Information (MGI)** the trader wants anchored in the product and agents over time. It also flags definitions and tooling that need a deliberate pass so language in prompts, MCP summaries, and `session_summaries` stays aligned with how *you* trade.

### Weekly MGI (Dalton-style weekly context)

Anchor: **weekly open each Sunday evening** (Globex week start for NQ — exact timestamp rule TBD vs exchange session calendar).

**Weekly Initial Balance (WIB)** — first balance window from that open (duration to confirm vs your chartbook; often first RTH-equivalent slice or first N hours of the week — document when locked in):

- WIB High, Mid, Low
- **50% extensions** up and down from WIB range
- **100% extensions** up and down
- **150% extensions** up and down
- **200% extensions** up and down

**Weekly VWAP:** VWAP **anchored from the weekly open** (distinct from session RTH VWAP).

**Other weekly / prior-week references:**

- Prior week high and low
- Prior week **close** (noted as “CI” in your notes — confirm symbol: close / settlement / last print)
- Weekly open level (current week)
- Current week: value area high, low, POC (TPO- or volume-based — align with pipeline default)
- Prior week: VAH, VAL, POC
- Prior week’s open
- **Current weekly mid-price** (define: midpoint of week range so far, mid of WIB, or other — lock when implementing)

**Verification note:** Cross-check each of the above against `levels` / TPO / VWAP pipelines and MCP tool payloads; flag any field that is missing, uses a different anchor (e.g. calendar week vs RTH week), or duplicates under another name.

### Daily MGI (RTH + Globex decomposition)

Much of this already exists in pipelines or session summaries; this list is the **coverage checklist** for documentation and agent narration.

**Volume / profile (RTH-scoped where noted):**

- Relative volume (RVOL) — session context
- **RVAH, RVAL, RPOC** — prior **RTH** session value area references (naming aligned to your chartbook)

**Prior / overnight structure:**

- Prior day high, prior day low
- **GVAH, GVAL, GPOC** — Globex (overnight) value area references for the relevant session
- **OVNH, OVNL** — overnight high / low (always tracked)

**RTH open and opening structures:**

- RTH open
- RTH opening range: high, low, mid
- RTH IB: high, low, mid
- RTH IB **100% extensions** (both directions)
- RTH IB **200% extensions** (both directions)
- **RTH VWAP**
- **RTH TWAP**

**Asia / London / combined Globex:**

- For **Asia** and **London** (and **combined Globex overnight** where applicable):
  - Opening range: high, low, mid
  - Extensions of each session’s OR (same extension ladder as IB or OR-only — specify when implementing)
  - IB (or equivalent first-balance window per session): high, low, mid
  - IB extensions per session if your chartbook uses them separately from OR

**Verification note:** Confirm session boundaries in code match Sierra/CME definitions you use visually; mismatches here break agent trust.

### TPO — poor highs and poor lows (definition pass)

We already surface **poor high** / **poor low** in places, but the doc and agents should **not** assume a single industry definition.

**Action:** Schedule a revisit to **write down the exact rule** used in The Desk (e.g. unfinished auction at extremes, single-print poor structure, minimum TPO count, multi-day context) and align:

- Pipeline / `session_summaries` field semantics
- Agent phrasing (“poor high” vs “weak high” vs “excess”)

Cross-reference: *Instrumentation Caveats* in [research-notes.md](research-notes.md) (sparse / incomplete poor flags in stored samples) — improving definitions may drive better instrumentation.

### Single prints

**Action:** Explicit review pass — how single prints are detected, stored, and narrated (including direction / context). Ensure setup ideas and `single_prints_direction` (or successor fields) are useful for research, not just display.

### RTH-only gaps

Track **gaps in price for RTH-only** continuity (open vs prior RTH close, prior RTH high/low, etc. — exact gap definition to match your chartbook).

**Use:** Regime context, gap-fill vs gap-and-go narratives, backtest hypotheses later.

### Absorption and initiation — event definitions and rules

Some of this likely exists in pipeline / agent text already; goal is **one canonical definition** for:

- **Absorption events** — what confirms absorption vs noise; invalidation; relationship to pace and delta
- **Initiation events** — initiative vs responsive framing; how initiation is distinguished from absorption failure or liquidity pull

**Action:** Draft explicit rules (even if discretionary) so the rules engine, events, and coaching agents use the **same vocabulary**.

### Iceberg-style behavior and stop runs

**Iceberg / hidden liquidity proxies:** Explore measurable signatures (repeated fills at same price, refresh of displayed size, footprint patterns) — may be partial / probabilistic only on tick data.

**Stop runs / stop-loss sweeps:** Define observable criteria (e.g. liquidity grab beyond level + immediate rejection, delta flip, pace spike) and separate from generic “spike” noise.

**Status:** Research / prototype — no claim yet that full iceberg detection is available; document intent for future tooling.

### Buy zones and sell zones

**Action:** Clarify **logic and inputs** for buy/sell zones (which levels, which flow confirmations, session scope). Review agent prompts so they don’t contradict pipeline math or each other.

### Average rotations, swing highs, swing lows

**Ideas to explore:**

- **Average rotation** — mean/median swing size in ticks or points over a lookback (session- or regime-scoped)
- **Swing high / swing low** — definition of pivot length, session vs multi-day, and how agents should cite them vs key levels / TPO structure

**Use:** Context for extension targets, mean reversion vs trend, and backtesting once definitions are stable.
