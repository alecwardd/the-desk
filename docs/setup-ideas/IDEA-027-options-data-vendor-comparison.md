---
id: IDEA-027
title: Options-Data Vendor Comparison — API-Accessible SPX Dealer-Flow (Interim Bridge)
status: Researched
regime: [any]
related: [IDEA-008, IDEA-013, IDEA-024, IDEA-026]
companionSpecs:
  - docs/phase-2-options-databento-memo.md
  - docs/setup-ideas/IDEA-026-volsignals-vs3d-vendor-eval.md
mcpPointers: []
hypothesisAnchor: false
---

# IDEA-027 — Options-Data Vendor Comparison (interim SPX bridge)

> Point-in-time vendor survey captured **2026-07-08**. This triages third-party
> options-data products for a possible **interim** intake bridge — **not** a
> validated Desk edge and not a purchase decision. Every vendor feature/price/
> methodology claim below is the vendor's own assertion unless a neutral source
> is cited, and is attributed as such. No trial was started, no account created,
> no payment entered, nothing subscribed. Several figures are secondary-sourced
> or paywalled and are flagged **`verify live`**.

<!-- stats: point-in-time -->

## Origin & decision reframe (why this supersedes IDEA-026's bottom line)

IDEA-026 evaluated one vendor (VolSignals VS3D) and returned **NO**, resting on
four legs. On 2026-07-08 Alec reframed the problem, and two of those legs fall:

1. **SPX-only is acceptable — even desirable.** The SPX options complex is far
   larger than NQ; its dealer positioning/flow is treated here as a *leading
   context* for NQ order flow, not a wrong-instrument mismatch. (Alec's call —
   `assumption`, not a backtested claim; the SPX→NQ lead should itself be
   validated before any signal is trusted.)
2. **Databento is deferred, not chosen-instead.** `docs/phase-2-options-databento-memo.md`
   (ADR-013) remains the eventual raw-data endgame, but its index-options
   licensing is too expensive until trading income is stable. So a cheap interim
   vendor is **complementary**, not conflicting.
3. **API access is now the #1 screening test.** the-desk is Rust + MCP; a vendor
   must return **structured data an agent can pull**, not a dashboard to eyeball.
   ConvexValue's API is specifically recalled as having worked for agents on SPX.
4. **Cost matters** — this is a bridge, so cheap + API beats pretty + polished.

Net: VS3D's verdict stays "no" *for the Desk* only because it is dashboard-only
(see §VolSignals). The **category** — API-accessible SPX dealer-flow as an
interim bridge — is now a reasonable buy. This note ranks the field on that.

## Ranking (API-accessibility → SPX coverage → cost → credibility)

| # | Vendor | API (agent-consumable?) | Underlyings | Data sourcing | Cost (`verify live`) | Interim fit |
|---|---|---|---|---|---|---|
| 1 | **cvforge** (ConvexValue Forge) | **Yes — API + MCP on the *free* tier**; agent-native (Claude Code/Codex) | All US options; **SPX** explicit | Pass-through "Massive" + FMP (disclosed); dealer-side classification undisclosed | **$0** / $29.99 / $59.99 mo | **Top pick** — cheap, agent-native, SPX |
| 2 | **Unusual Whales** | Yes — REST API **+ MCP server**, OpenAPI; pre-computed `/stock/SPX/greek-exposure` (GEX/DEX/vanna/charm) | SPX + equities | Proprietary (inferred) | ~$150–250+/mo | Strong — turnkey pre-aggregated exposures, pricier |
| 3 | **GEXBot** (gexbot.com) | Yes — REST **+ WebSocket**, Bearer auth, OpenAPI spec | SPX confirmed; others behind `/tickers` (unverified) | OI-sign convention (inferred) | ~$99/mo + $50 orderflow | Solid API-first; thin credibility |
| 4 | **MenthorQ** | Partial — "API key" is a **charting-platform entitlement**, not raw REST | **Native NQ/ES/RTY futures** gamma + SPX | Undisclosed (inferred) | $129/mo | Best NQ-native gamma, but not agent-callable raw |
| 5 | **Cboe DataShop** (All Access API) | Yes — official raw OPRA API | SPX/OEX (index license) | Exchange-official raw (compute GEX yourself) | ~$1k+/mo index license | Too costly interim; = a Databento peer, not a bridge |
| — | **VolSignals VS3D** | **No** (all tiers) | SPX/VIX | Participant-attributed *claim* (best-grounded) | $299/mo (+ up to $10k VIP) | Out — dashboard-only |
| — | **SpotGamma** | **No** (platform integrations only) | SPX/SPY | OI-sign + proprietary adj (inferred) | $99 / $299 mo | Out for pipeline — best pedigree though |
| — | **SPXOmega** | **No** (Discord + ToS overlays) | SPX/ES/SPY/QQQ + some equities | Undisclosed | ~$49–150 est. (unconfirmed) | Out — no API, low credibility |

## Per-vendor notes (concise)

**cvforge / ConvexValue** — Forge is a free, agent-native desktop app on the same
backend as the classic Terminal; crucially it moves **API + MCP access to the
free $0 tier** (20 req/hr; $29.99 → 1k/hr; $59.99 → 50k/hr + full historical US
options). Exposes chains, greeks, IV, volume/OI, flow/TAS; SPX has a dedicated
report page. Sources options data from a named pass-through vendor ("Massive")
+ FMP — disclosed — but the **dealer-side/gamma classification math is
undisclosed** (treat any positioning signal as black-box). A separate older
community Python wrapper (`getChain()`/`getFlowTable()`/`tasQuery()`, email/pass
auth) matches Alec's "API calls worked" memory. Built/run by a solo operator
(Juan Bernardo Tobar) publishing since ~2020; no red flags, but external
reputation is thin. **Gap:** no first-party API schema/endpoint reference found —
docs are marketing-thin. → *validate hands-on with a live free key before any
Rust/MCP build.* Sources: [cvforge](https://cvforge.convexvalue.com/),
[pricing](https://cvforge.convexvalue.com/pricing),
[SPX report](https://convexvalue.com/reports/SPX/options-flow/),
[launch post](https://convex.substack.com/p/cvforge),
[community wrapper](https://github.com/aaronsdevera/convex-value-python) — all accessed 2026-07-08.
`verify live`: Terminal tier pricing (go.convexvalue.com unreachable) and the
full API reference depth.

**Unusual Whales** — the best-*verified* agent-consumable option: real REST API
**and an MCP server**, OpenAPI spec, with **pre-computed** SPX exposures
(`/api/stock/SPX/greek-exposure`, `/spot-exposures/strike` for GEX/DEX/vanna/
charm) — i.e. you don't have to derive gamma yourself. ~$150–250+/mo depending on
tier. Inferred (proprietary) sourcing. If cvforge's API turns out thin or the
free tier too rate-limited, this is the turnkey fallback. Source:
api.unusualwhales.com/docs (accessed 2026-07-08). `verify live`: exact tier that
unlocks the greek-exposure endpoints + price.

**GEXBot** — documented REST + WebSocket (Zstd protobuf) API, Bearer-token auth,
public OpenAPI spec ([github.com/nfa-llc/gexbot-openapi](https://github.com/nfa-llc/gexbot-openapi)),
tier-gated endpoint families (`/{ticker}/classic`, `/state`, `/orderflow`); SPX
confirmed in examples. ~$99/mo (+$50 orderflow) per secondary sources — the site
is a JS SPA that resisted fetch, so **`verify live`**. Data is standard OI-sign
(inferred). Credibility only moderate: anonymous domain reg, entity "NFA LLC," no
named founder (ScamAdviser "probably legit"). Cheapest dedicated GEX-with-API if
cvforge doesn't pan out. Accessed 2026-07-08.

**MenthorQ** — worth flagging specifically because it computes gamma **natively on
NQ/ES/RTY futures** (not just SPX) — the only surveyed vendor that does. But its
"API key" is an entitlement for partner charting platforms (Quantower/TrendSpider/
ATAS/NinjaTrader/Sierra), **not** a generic REST/JSON API a Rust system can call.
$129/mo, undisclosed sourcing. Keep on the radar for NQ-native context if it ever
ships a real API. Accessed 2026-07-08.

**Cboe DataShop (All Access API)** — exchange-official raw OPRA; highest data
credibility, but no pre-built GEX (compute yourself) and SPX/OEX needs a CGI
index license (~$1k+/mo). This is a Databento *peer*, not an interim bridge —
revisit alongside ADR-013 when the budget exists. Accessed 2026-07-08.

**VolSignals VS3D** — re-checked all tiers (VS3D $299, VS Pro $300, VolStudies
$997, RTM trial $285, VIP $10k): **no API/export/webhook anywhere**. Confirms
IDEA-026 — dashboard-only, best-grounded sourcing *claim* (participant-attributed)
but unusable by Desk agents. Fine as a *personal* discretionary tool only. Source:
[pricing](https://www.volsignals.com/membership-area/pricing) (accessed 2026-07-08).

**SpotGamma** — best pedigree (Brent Kochuba, credited GEX originator) and arguably
best-known data, but **no retail API** — delivery is dashboard + third-party
platform integrations (TrendSpider/NinjaTrader/Sierra/ToS). Fails the pipeline
test unless an institutional feed is negotiated (sales-gated, unverified).
$99/$299. Accessed 2026-07-08.

**SPXOmega** — Discord + ThinkOrSwim overlay product; no API found; site
Cloudflare-gated (low-confidence, indexed snippets only); single-operator, no
track record. Not a fit. Accessed 2026-07-08.

## Recommendation

**Start a free cvforge account and validate its API/MCP hands-on** — it is the
only surveyed product that is simultaneously (a) API + MCP accessible, (b) at
$0 to prove out, and (c) explicitly SPX-covered, and it matches Alec's prior
"the API worked" experience. It clears the two hard bars (agent-consumable +
cheap) that everything else trades off.

- **If cvforge's API reference/rate limits prove too thin** for a real pipeline →
  **Unusual Whales** (best-verified API + MCP, pre-computed SPX exposures) is the
  turnkey paid fallback; **GEXBot** the cheaper REST+WS alternative.
- **Data-quality caveat that applies to all of them except Cboe:** the dealer
  positioning/gamma is *inferred* (OI-sign or an undisclosed model). Only VS3D
  even *claims* participant-attributed data, and it has no API. So whatever is
  ingested should be treated as a **black-box context input, cross-checked** —
  consistent with IDEA-024's rule against claiming hidden participant intent —
  not a ground-truth position feed. That truth only comes with the raw
  Databento/Cboe path later.
- **Sequence:** cvforge free-key spike → confirm response schema + SPX fields an
  MCP tool can return → tiny read-only prototype → decide paid tier / fallback.
  No subscription until the free spike proves the schema.

## 2026-07-08 update — cvforge API access, corrected (hands-on + docs read)

Alec created a cvforge account, and the public API docs were read directly
([cvforge API docs](https://cvforge.convexvalue.com/docs/api/), accessed
2026-07-08). This corrects the free-tier claim in the ranking/§cvforge above:

- **API is real and publicly documented.** Base URL
  `https://tap.convexvalue.com/api/data`; auth = Bearer token `cv_live_…`.
  11+ endpoints incl. `/chains` (option chains), `/query` (SQL over options),
  `/screen`, `/mas/*` (historical OHLC), `/fmp/stable/*` (fundamentals), `/ai/*`.
  Params include `strike_price`, `implied_volatility`, `delta`. JSON responses.
- **BUT "API on the free tier" was overstated.** In practice a usable `cv_live_`
  data key required subscribing to a plan — Alec could **not** get an API key on
  the bare free account. Core data endpoints (`/chains`, `/query`, `/mas`) are
  **plan-gated**; Free's 20 req/hr is heavily limited and historical is
  Research-tier only. Only the `/ai/*` routes are not plan-gated (prepaid AI
  credits, metered).
- **No advertised free trial.** Cheapest data tier = **Go $29.99/mo**
  (month-to-month, cancel anytime).

**Revised recommendation.** cvforge is still the best *architecture* fit
(documented REST + SPX + agent/MCP-oriented, cheap) but is **not free to
validate live**. Two-phase next step: (1) a **docs-based schema validation needs
no key and no payment** — map the documented endpoints/fields to the-desk's
needs, confirm SPX coverage/history, and produce a go/no-go + integration plan;
(2) to smoke-test live, either ask ConvexValue (solo dev, reachable via
Substack/X) for a short eval key, or take one month of Go ($30) — **not until
Alec approves spend**. Fallback unchanged: Unusual Whales → GEXBot (re-check
their trial/entry terms if pursued).

## Phase 1 — cvforge docs-based validation (no key, no pay) — 2026-07-08

Validated cvforge against the-desk's actual Phase-2 options needs
(`docs/phase-2-options-databento-memo.md`, ADR-013) using only public docs
([API](https://cvforge.convexvalue.com/docs/api/),
[options snapshot schema](https://cvforge.convexvalue.com/docs/data/options-snapshot),
[classic params](https://convexvalue.com/docs/params), all accessed 2026-07-08).
**No key created, nothing subscribed, no code written.**

### Endpoints available (from docs)
- `GET/POST /chains/{symbol}?params=…` — per-contract option-chain snapshot (all plans).
- `POST /screen` — cross-market screener over every contract (all plans).
- `POST /query` — read-only SQL over the options snapshot (all plans).
- `GET/POST /mas/v2/aggs/…`, `/mas/v1/open-close/…` — historical OHLC (**Research tier**).
- `POST /mcp` — MCP server, JSON-RPC 2.0, for coding agents (all plans).
- `/fmp/stable/*` fundamentals; `/ai/*` model access (credit-metered). Auth: Bearer
  `cv_live_`. Rate limits/hr: Free 20 · Go 1,000 · Research 50,000.

### Required fields for the-desk SPX dealer-flow work (per ADR-013 GEXAggregator)
Per-contract: strike · expiration · option type · open interest · IV · **gamma** (+ delta) ·
underlying price · contract multiplier → the-desk computes `GEX_strike = gamma × OI ×
mult × sign(dealer)`, then zero-gamma / max-gamma / GEX-flip / dealer-positioning
itself. charm & vanna wanted for the optional charm/vanna-flow (P1).

### What cvforge appears to provide (maps cleanly)
- **`/chains` snapshot for `I:SPX` (index options via `I:` prefix — SPX/VIX/NDX/RUT/XSP
  explicitly supported).** Fields incl. `strike_price`, `expiration_date`,
  `contract_type`, `open_interest`, `implied_volatility`, `delta`, `gamma`, `theta`,
  `vega`, `shares_per_contract`, `underlying_price`, NBBO + day OHLCV. → **every raw
  input the GEXAggregator needs is present.**
- **Classic params system** adds `gamma`, `charm`, `vanna`, and the products `gxoi`
  (gamma×OI), `gxvolm`, `charmxoi`, `vannaxoi`, etc., at contract **and** underlying
  level — reachable via `/query`/`/screen`. So charm/vanna and a ready-made `gxoi`
  are obtainable, just not in the default `/chains` payload.

### Missing / unclear (the reasons a key is still needed)
- **charm & vanna are NOT in the `/chains` snapshot schema** — only in the params
  system (`/query`). Confirm they resolve for `I:SPX` on the **Go** tier, not just Research.
- **No signed dealer-GEX aggregate** — cvforge gives raw gamma/OI; the-desk applies its
  own `sign(dealer)`. (This is actually *better* ADR-013 alignment than a black-box GEX
  vendor — but it means no turnkey "dealer positioning" field.)
- **Real-time vs delayed** greeks/quotes on Go unclear; **0DTE completeness** (all SPXW
  daily expirations + full strike ladder) unverified; **historical depth** is
  Research-tier (`/mas`) — matters for the memo's SQLite/backtest persistence.
- **Rate limits:** Free 20/hr is too low for a full SPX chain refresh; Go 1,000/hr is
  likely fine for a 5-min RTH refresh — confirm a full chain = one request, not many.

### Expected integration shape
Drops into the memo's planned `options/` module with **less** work than Databento:
swap `DatabentoClient` → a `CvforgeClient` (REST, `GET /chains/I:SPX?params=…`, Bearer
`cv_live_`); **skip most of `GreeksEngine`** (delta/gamma are vendor-provided — pull
charm/vanna via `/query` or keep computing them to stay ADR-013-pure); feed
`gamma × open_interest × shares_per_contract × sign(dealer)` into the existing
`GEXAggregator`; emit the same `OptionsState` → `MarketState` → `ConditionField`s
(`gex_level`, `gamma_exposure_sign`, `dealer_positioning`). The `/mcp` endpoint also
lets agents query cvforge directly. Trade-off: vendor-computed greeks are the black box
ADR-013 exists to replace — acceptable for an **interim** bridge, revisited at Databento.

### Go / No-Go (before any paid month)
**Conditional GO.** The docs clear the architectural fit: `I:SPX` is supported and every
raw field the-desk's own GEX model needs is in `/chains`, over a REST + MCP surface, at
low cost — a *better* interim fit than the finished-GEX vendors because the-desk keeps
its own aggregation. This is **not** a blind buy: one month of **Go ($29.99, cancel
anytime)** — or an eval key — should confirm the four unknowns above (charm/vanna via
`/query` on Go; real-time vs delayed; 0DTE completeness; a full-chain pull fits Go's
rate limit). Take **Research ($59.99)** instead only if historical/backtest persistence
is needed now. If charm/vanna or real-time turn out Research-gated or absent, re-weigh
against **Unusual Whales** (turnkey pre-computed GEX + charm + vanna). Recommendation:
proceed to a paid **Go**-tier Phase-2 smoke-test (or eval key) — do not stay blocked, but
do not build the integration until that smoke-test confirms the unknowns.

## Appendix — eval-key request draft (UNSENT; Alec to send via X)

> Hi Juan — I'm building a systematic NQ-futures trading system and evaluating cvforge
> as an interim SPX options/dealer-flow data source (I want the raw per-strike
> gamma/OI/IV from `/chains` on `I:SPX`, plus charm/vanna via `/query`, to compute GEX
> myself). The docs look like a strong fit. Before committing to a plan, would you be
> open to a short temporary API key so I can validate the `I:SPX` schema, charm/vanna
> availability, and rate limits against my pipeline? Happy to share what I find. Thanks
> for building this — the API-first + MCP design is exactly what I was looking for.

## Top risks / caveats

- **SPX→NQ lead is an unproven assumption**, not a validated edge. Validate the
  relationship before any signal derived from SPX flow is trusted for NQ.
- **Inferred data everywhere** (except raw OPRA): don't over-trust vendor gamma.
- **Verification gaps this turn:** cvforge full API spec + Terminal pricing;
  GEXBot pricing/ticker list/rate limits (JS SPA); Unusual Whales exact
  endpoint-unlocking tier; SPXOmega everything (Cloudflare). All marked
  `verify live` — re-check before any spend or build.
- **Account creation / trials touch an external service** → Alec's call, per
  AGENTS L0 (agents don't create accounts or enter payment).

## See also

- Prior single-vendor eval (superseded framing): [IDEA-026](IDEA-026-volsignals-vs3d-vendor-eval.md)
- Eventual raw-data endgame (deferred on cost): [phase-2-options-databento-memo.md](../phase-2-options-databento-memo.md)
- Related gamma/positioning ideas: [IDEA-008](IDEA-008-0dte-gamma-regime.md), [IDEA-013](IDEA-013-gamma-gated-setup-overlay.md), [IDEA-024](IDEA-024-market-maker-pressure-inference.md)
- Setup ideas index: [index.md](index.md)
- Vault source card: `raw/sources/2026-07-08-volsignals-vs3d.md`
