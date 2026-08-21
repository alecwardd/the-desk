---
id: IDEA-032
title: Hidden Markov Models lecture notes — docs-only repo-fit assessment
status: Researched
regime: [any]
related: [IDEA-000, IDEA-007, IDEA-030, IDEA-031]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/setup-ideas-and-backtesting.md
mcpPointers: []
hypothesisAnchor: false
---

# IDEA-032 — Hidden Markov Models lecture notes (repo-fit)

> Point-in-time **docs-only** assessment captured **2026-07-22**. Source PDF
> read in full for this note. This is **not** a setup, not a playbook rule,
> not a live agent signal, and not authorization to implement HMM inference
> in Rust/MCP. Author claims below are attributed; no edge is asserted.

<!-- stats: point-in-time -->

## Origin

- Trader capture: vault inbox note “hidden markov models” (untracked;
  triaged 2026-07-17). Alec asked whether the linked notes suggest anything
  to implement for the trading repo / agents, and what kinds of things could
  be made from them.
- Vault source card:
  `second-brain/raw/sources/2026-07-17-rithmic-and-household-research.md`
  (HMM section).
- Primary source (accessed 2026-07-22):
  https://jwmi.github.io/ASM/5-HMMs.pdf
- Attribution: Jeffrey W. Miller (2016), *Lecture Notes on Advanced Stochastic
  Modeling*, Duke University. Licensed CC BY-NC-ND 4.0. Educational lecture
  notes — **not** an empirical NQ/futures trading paper.

## What the source actually is

Miller’s notes teach the standard discrete-state HMM toolkit:

1. **Model** — hidden Markov chain \(Z_t\) with observed emissions \(X_t\);
   time-homogeneous transition matrix \(T\); emission distributions
   \(\varepsilon_i\); initial distribution \(\pi\).
2. **Viterbi** — most probable state path given observations (\(O(nm^2)\)).
3. **Forward–backward** — marginal state / transition posteriors and sampling
   from \(p(z_{1:n}|x_{1:n})\).
4. **Baum–Welch (EM)** — parameter estimation when \(T\), \(\pi\), and
   emissions are unknown.
5. **Numerical hygiene** — log-space Viterbi and log-sum-exp to avoid
   underflow.

The notes briefly list “financial data” among sequential domains where HMMs
are used. They do **not** provide NQ futures evidence, session definitions,
transaction-cost treatment, walk-forward design, or any claimed Sharpe /
win-rate result. Any trading performance claim elsewhere (including older
IDEA-007 hub prose citing academic Sharpes) is **out of scope for this PDF**
and must not be imported as fact from this assessment.

## Verdict: ADAPT (do not adopt; do not skip)

| Option | Meaning here | Result |
|--------|--------------|--------|
| **Adopt** | Implement Miller’s algorithms as live Desk regime / agent signals | **No** |
| **Adapt** | Keep as attributed methodology reference for a future *offline* research design under the existing regime track | **Yes** |
| **Skip** | Discard; no further reference value | **No** |

**Why adapt, not adopt**

- The Desk already has a **deterministic** session regime classifier in
  `src/pipelines/regime.rs` (`oneSidedAcceptance` / `migration` /
  `transition` / `unclear`) used as a setup-family gate (IDEA-000). That is
  independently testable Layer-1 math — compatible with CLAUDE.md’s
  never-generate-proprietary-signals rule.
- IDEA-007 already sketched a simpler **RV-ratio** microstructure regime
  detector first, with HMM listed as a later advanced option. This PDF is
  useful background for that *later* track; it does not jump the queue past
  the simpler baseline.
- Live agent value, if any, would be **context labels** (posterior state
  probabilities, regime dwell, transition hazard) after a dated offline
  backtest clears the Research Sample Size Policy — never a trade trigger
  invented from the lecture notes.

**Why not skip**

- Alec explicitly wants deep research on this class of models.
- The algorithms are the right vocabulary if/when the Desk tests latent
  regime labels against existing deterministic regimes and setup outcomes.

## Fit against current Desk surfaces

Already present / preferable baselines:

- Session regime gate: `Regime` in `pipelines/regime.rs` + rules-engine
  `regime` field.
- Participation / vol context: RVOL, tape pace, day type, balance state,
  IB extension state (see MCP live tools).
- Research/history path: session summaries + signal-outcome queries after
  backfill (historical MCP tools).
- Related research queues: IDEA-007 (microstructure regime), IDEA-030
  (balance taxonomy), IDEA-031 (compression/expansion).

Gaps relative to a future HMM experiment (not requirements to build now):

- No in-repo HMM / EM library path is assumed by this note.
- Emission feature design (returns, RV, imbalance, pace, etc.), state count,
  session scope, and leakage controls are undefined.
- Live MCP must not expose an HMM label until an offline verdict exists.

## Falsifiable hypotheses (research only)

These are **candidates for a later backtest plan**, not current beliefs.

1. **H1 — Label agreement:** On RTH NQ sessions with coverage
   \(N \ge 30\), a 2- or 3-state HMM fit offline on session-scoped features
   agrees with the deterministic `Regime` label on a pre-registered metric
   (e.g. adjusted Rand / confusion matrix) above a chance baseline.
   *Falsify if agreement ≤ chance after multiple random seeds / EM inits.*

2. **H2 — Filter lift vs RV-ratio baseline:** Conditioning an existing
   setup’s outcomes on HMM “rotation-like” states improves the R-distribution
   versus (a) ungated and (b) IDEA-007’s simpler same-time-of-day RV-ratio
   gate, with \(N \ge 30\) per bucket and explicit costs/slippage policy.
   *Falsify if HMM gate does not beat both baselines on the pre-registered
   primary metric.*

3. **H3 — Transition hazard is informative:** Forward–backward
   \(P(Z_t \neq Z_{t+1}|x_{1:t})\) (causal/filtering form, not full-sample
   smoothing into the future) predicts short-horizon range expansion better
   than raw RV or tape-pace acceleration alone.
   *Falsify if incremental AUC / Brier skill ≤ 0 vs those baselines.*

4. **H4 — Agent usefulness without signals:** Providing agents a
   *posterior-only* regime packet (state probs + dwell + data-quality flags)
   changes neither fill simulation nor playbook alerts, but improves
   human-rated context usefulness in a small labeled review set.
   *Falsify if reviewers find no incremental clarity over existing
   `get_market_snapshot` / day-type / RVOL context.*

No hypothesis above authorizes live alerts, sizing, or autopilot behavior.

## Data requirements (before any code or backtest)

Minimum research packet — verify coverage first; do not invent missing data:

1. **Instrument / scope:** NQ (or MNQ explicitly labeled); separate RTH vs
   Globex; never silently mix.
2. **Feature vector \(x_t\):** pre-register fields available from Desk storage
   or derivable without peeking at future ticks (e.g. log returns, realized
   vol, signed volume / delta, RVOL ratio, pace percentile). Document bar
   size (1-min vs volume bars) and timezone.
3. **Sample:** enough sessions for \(N \ge 30\) *per compared bucket* after
   walk-forward splits; follow AGENT.md Research Sample Size Policy.
4. **Leakage controls:** train/EM only on past folds; for live-like
   evaluation use filtering (forward only), not full-sample smoothing that
   sees future emissions.
5. **Baselines locked first:** deterministic `Regime`; IDEA-007 RV-ratio
   sketch; ungated setup outcomes.
6. **Costs / realism:** any outcome study must state whether it is
   event-study (market movement vs levels) vs fill simulation; do not quote
   academic Sharpes from other papers as Desk evidence.
7. **Provenance:** store seed, EM init policy, state-count selection rule,
   and code/data commit hashes in the eventual backtest job record.

## What agents could eventually make (only after a dated verdict)

- Offline research notebooks / Rust research jobs that emit attributed
  regime posteriors into SQLite for historical queries.
- MCP **context** fields (not alerts): e.g. `hmm_state_posterior`,
  `hmm_transition_hazard`, with `trust` / staleness / training-window
  metadata.
- Coach-language constraints: agents may say “latent-state model currently
  favors X with probability p under training window W” — never “buy/sell
  because HMM says so.”

Until then, agents should keep using deterministic regime + existing
microstructure tools.

## Explicit non-goals (this assessment)

- No Rust/Python HMM implementation in this pass.
- No dependency add (`nalgebra` or otherwise).
- No MCP tool, playbook template, or live MarketState field.
- No backtest run, no performance numbers, no signal claim.
- No restart of live MCP/storage; no account or data-vendor action.

## Top risks / caveats

- **Label fiction:** EM can invent plausible states that do not map to
  auction/order-flow concepts the trader recognizes.
- **Non-stationarity:** time-homogeneous \(T\) is a strong assumption across
  Asia/London/RTH and macro days.
- **Look-ahead:** smoothing posteriors are easy to misuse in “live” narratives.
- **Complexity tax:** Miller’s notes are clean theory; productionizing them
  is a large engineering + validation project relative to extending
  `regime.rs` and IDEA-007’s simple detector.
- **License:** CC BY-NC-ND 4.0 — fine for internal study notes; do not
  republish the PDF as Desk product docs.

## Recommended next step (human or later agent)

1. Leave IDEA-007’s “start with RV-ratio” path as the active regime-research
   priority.
2. If Alec wants HMM work next, authorize a **separate** backtest-design
   note that locks H1–H3, features, splits, and coverage checks — still no
   implementation until that design is approved.
3. Do not schedule live agent exposure before a dated offline verdict.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-032](../setup-ideas-and-backtesting.md#idea-032)
- Setup index: [index.md](index.md)
- Related: [IDEA-007](IDEA-007-microstructure-regime-detection.md) (microstructure
  regime), [IDEA-000](IDEA-000-regime-gated-selector.md),
  [IDEA-030](IDEA-030-nq-balance-zone-taxonomy.md),
  [IDEA-031](IDEA-031-session-range-compression-expansion.md)
- Pipeline: [`src/pipelines/regime.rs`](../../src/pipelines/regime.rs)
- Vault source card: `raw/sources/2026-07-17-rithmic-and-household-research.md`
