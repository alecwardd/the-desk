---
id: IDEA-026
title: VolSignals VS3D — SPX/VIX Dealer Positioning Vendor Evaluation
status: Researched
regime: [any]
related: [IDEA-008, IDEA-013, IDEA-024]
companionSpecs:
  - docs/phase-2-options-databento-memo.md
mcpPointers: []
hypothesisAnchor: false
---

# IDEA-026 — VolSignals VS3D Vendor Evaluation

> Point-in-time vendor evaluation captured **2026-07-08**. This is a triage of a
> third-party product for possible options-data intake, **not** a validated Desk
> edge and not a purchase decision. Every vendor performance/feature claim below
> is the vendor's own assertion and is attributed as such with a falsification
> note. No trial was started, no payment entered, nothing was subscribed.

<!-- stats: point-in-time -->

## Origin

Captured from Alec's phone clipping of the VS3D landing page (vault
`raw/inbox/2026-07-08-0555-volsignals-clipping.md`), source card at
`raw/sources/2026-07-08-volsignals-vs3d.md`. Alec's note: "wonder if a free
trial would be good for the-desk to pull in options data, market making
strategies, etc." This note answers that question.

## Sources verified (all accessed 2026-07-08)

- Product landing / features / pricing / FAQ: https://vs3d.volsignals.com/home
- Getting-started guide: https://vs3d.volsignals.com/home/getting-started
- Parent site: https://www.volsignals.com/ and https://www.volsignals.com/about-us
- Methodology claim post (X): https://x.com/VolSignals/status/2012528863788531742 (post body behind login/paywall this turn; claim text captured via search-index summary, not first-party read)
- Account: https://x.com/volsignals
- Trustpilot listing: https://www.trustpilot.com/review/volsignals.com (HTTP 403 this turn — could not read review bodies)

> Verification gap: the two most decision-relevant primary pages — the X
> methodology thread and the Trustpilot reviews — were not first-party readable
> this turn (login-wall / 403). Claims sourced from them are marked as
> second-hand below and should be re-checked before any purchase.

## What VS3D is

Browser-based, real-time analytics dashboard visualizing claimed market-maker
positioning for **SPX and VIX options only** (S&P 500 index + VIX; no NDX, no NQ
futures options). Views: Gradient Chart (gamma/charm/vanna over price),
Positions by Strike, Position Grid heatmap (strike × expiration), Positions by
Expiration, custom multi-panel dashboards, historical playback (3 months on the
Standard plan). Ships with a Discord + daily premarket analysis community layer.
Source: landing page, accessed 2026-07-08.

## Who is behind it (credibility)

Founders present verifiable institutional market-making pedigree, which makes the
"built by actual market makers" framing credible rather than pure marketing:

- **Daniel Roos** — ex-CBOE floor, former lead Index Trader / youngest Capital
  Partner at Belvedere Trading (large US index MM firm), >decade as SPX
  market-making desk head. Owner/CEO.
- **Matthew Nadel** — ~three decades equity-derivatives desk experience (JPM,
  Morgan Stanley, SocGen, RBC, Credit Suisse, UBS), SPX/VIX books.

Source: https://www.volsignals.com/about-us, accessed 2026-07-08. Backgrounds are
self-reported but name checkable FINRA/LinkedIn profiles; not independently
verified in this pass.

## Acceptance criteria

### 1. Position-data sourcing methodology — disclosed or inferred?

**Partially disclosed at the concept level; the actual attribution model is
proprietary and not independently verifiable.**

- VolSignals **asserts** VS3D uses "real positions, not IV-inferred" — derived
  from OCC/CBOE **exchange-level clearing data broken down by participant type**
  (customer buys, non-customer buys, firm sales, market-maker sales), so they
  claim to know both *who is hedging* and *whether the hedger is long or short*
  "for every single SPX contract." Source: vendor landing FAQ + X methodology
  post (second-hand, see verification gap), accessed 2026-07-08.
- **Falsification / assessment:** Participant-type clearing data is a real,
  known category — the OCC does segment volume by customer/firm/market-maker
  (https://www.theocc.com/market-data, accessed 2026-07-08). If VS3D genuinely
  licenses the granular, participant-attributed feed, its read is **materially
  better grounded than the common retail GEX heuristic** (open interest × a
  hard-coded `sign(dealer)` assumption). BUT: (a) turning attributed *signed
  volume* into *net position by strike* still requires an accumulation model and
  open-vs-close / hedging assumptions that VS3D does **not** publish; (b) the
  specific data vendor, licensing, and math are undisclosed; (c) it is not a
  literal read of any dealer's book. Net: treat "actual positions" as a
  **strong, plausible, but unverifiable-without-subscribing** claim — better than
  IV-surface inference, still a proprietary black box. Cannot be falsified from
  outside the paywall.

### 2. Overlap vs the-desk's existing pipelines / options research

- **Instrument mismatch is the core problem.** the-desk trades **NQ futures**
  (Nasdaq) off Sierra `.scid` tick data. VS3D covers **SPX/VIX only**. SPX dealer
  gamma is a *correlated backdrop* to NQ, not the same underlying — usable as
  discretionary context, not as instrument-accurate data.
- **the-desk has already chosen a conflicting options-data path.**
  `docs/phase-2-options-databento-memo.md` (ADR-013, dated 2026-03-05) makes
  **Databento** the preferred provider precisely because it delivers **raw**
  OPRA (NDX/SPX) + CME (NQ futures options) chains so the-desk can **compute all
  Greeks and GEX itself in Rust — explicitly "no black-box vendor Greeks."**
  VS3D is the opposite of that architecture: a finished vendor-Greeks dashboard.
- **Conceptual (not integration) overlap** with the gamma/positioning idea
  cluster: IDEA-008 (0DTE Gamma Regime), IDEA-013 (Gamma-Gated Setup Overlay),
  IDEA-024 (Market-Maker Pressure Inference). Note IDEA-024 is a *deterministic
  Rust* DOM/tape inference layer for NQ and explicitly defers any options/dealer
  overlay to "future Phase 2, not required for v1." VS3D could *inspire* the
  dealer-sign approach (replacing `sign(dealer)` assumptions with attributed
  positions) but cannot *feed* any of these because it exposes no data.
- **Architecture conflict:** CLAUDE.md requires market-data math to live in Rust
  and MCP tools to return structured data; a dashboard product cannot satisfy
  either.

### 3. API vs dashboard-only

**Dashboard / browser-only. Eyeballs, not a pipeline.** Vendor FAQ: "web-based
platform that runs in any modern browser. There's nothing to download or
install." No API, data export, developer docs, or programmatic access found in
the landing page, getting-started guide, or targeted search (accessed
2026-07-08). This is the decisive point against Alec's stated goal ("pull in
options data"): VS3D cannot feed the-desk's automated Rust pipeline, backfill, or
backtests. It is a discretionary viewing tool only.

### 4. Cost after trial

7-day free trial on all plans, month-to-month, cancel anytime. **VS3D Standard =
$299/mo** (all core views, SPX & VIX, gamma/charm/vanna gradients, custom
dashboard, 3 months historical playback). A higher-tier "Pro Bundle" adds analyst
commentary / alerts / key levels / price targets at an **undisclosed** (higher)
price. Source: vendor pricing/FAQ, accessed 2026-07-08.

### 5. Recommendation

**No — do not trial VS3D as an options-data source for the-desk.** Reasoning:

1. **Dashboard-only, no API** → structurally cannot feed the-desk's Rust
   pipeline / backtests. This alone disqualifies it for the stated purpose.
2. **Wrong instrument** → SPX/VIX only; the-desk trades NQ. Correlated, not
   accurate.
3. **Direct architecture conflict** → the-desk's own ADR-013 (Databento) exists
   specifically to compute Greeks/GEX in-house and avoid black-box vendor Greeks,
   with NQ/NDX coverage VS3D lacks, at lower/controllable cost (~$199/mo live +
   cheap historical vs $299/mo eyeballs-only).
4. **$299/mo recurring** for a viewing tool that produces no ingestible artifact
   is poor value for this repo's goal.

**Honest nuance:** VS3D looks like a *credible, high-quality discretionary
product* (real ex-MM founders, plausible clearing-data methodology, positive
community signal). If Alec wants it for **personal discretionary use** — an
SPX/VIX dealer-flow "weather report" and options/vol education from experienced
market makers — a one-week free trial is low-risk and could be worthwhile. That
is a *personal* decision, separate from and not blocking the-desk's engineering
path. Just don't buy it expecting it to become a data source here.

## 2026-07-08 addendum — reframed by Alec's constraints (read before quoting the verdict)

Alec reframed the problem after this note was written, which changes what the
"NO" means. Two of the four disqualifiers above no longer apply at the category
level:

- **SPX-only is now acceptable** (SPX complex treated as leading context for NQ
  flow — an `assumption` to validate, not a mismatch).
- **Databento is deferred on cost**, so a cheap interim vendor is complementary,
  not conflicting with ADR-013.

What stands: **VS3D specifically remains NO for the Desk because it is
dashboard-only (no API/export at any tier — re-confirmed 2026-07-08).** It cannot
feed a Rust/MCP pipeline regardless of the reframe. The *category* (API-accessible
SPX dealer-flow as an interim bridge) is now worth buying — see the full ranked
survey in **[IDEA-027](IDEA-027-options-data-vendor-comparison.md)**, where
cvforge (free-tier API+MCP), Unusual Whales, and GEXBot lead. VS3D stays a
reasonable *personal* discretionary tool only.

## Top risks / caveats

- "Actual positions, not inferred" is an **unverifiable proprietary model** —
  marketing may overstate certainty; the accumulation/hedging assumptions are
  hidden. Falsifiable only from inside a subscription.
- **SPX ≠ NQ** proxy risk if used as NQ context.
- **Verification gaps this turn:** X methodology thread (login-wall) and
  Trustpilot reviews (403) not first-party read — re-verify before any spend.
- No independent third-party accuracy audit was located.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-026](../setup-ideas-and-backtesting.md#idea-026)
- Setup ideas index: [index.md](index.md)
- Phase 2 options direction (conflicting path): [phase-2-options-databento-memo.md](../phase-2-options-databento-memo.md)
- Related gamma/positioning ideas: [IDEA-008](IDEA-008-0dte-gamma-regime.md), [IDEA-013](IDEA-013-gamma-gated-setup-overlay.md), [IDEA-024](IDEA-024-market-maker-pressure-inference.md)
- Vault source card: `raw/sources/2026-07-08-volsignals-vs3d.md`
