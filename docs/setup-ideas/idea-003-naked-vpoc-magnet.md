# IDEA-003: Naked VPOC Magnet Trade

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Auction Market Theory, volume profile analysis
**Complements:** Single Print Continuation (tpl_single_print_continuation), Session Inventory (tpl_session_inventory_clear)

**Concept:** Track POCs from prior sessions that price has not revisited ("naked" VPOCs). These act as price magnets — the market tends to gravitate toward unreconciled fair value.

**Setup — Naked VPOC Fill:**
- Maintain list of naked VPOCs from prior 5-10 sessions
- Entry: When developing profile + delta direction aligns toward a naked VPOC, enter on pullback
- Stop: Below nearest HVN cluster or developing VAL
- Target: The naked VPOC itself
- Statistics: ~6 exact VPOC bounces/month on index futures; 75%+ fill rate over multi-day horizon

**Setup — POC Magnet Mean Reversion:**
- Context: Price moves 60+ NQ points away from developing POC in a session
- Entry: First reversal signal (rejection candle, delta divergence) toward POC
- Stop: Beyond reversal extreme
- Target: POC level
- Win rate: 75%+ in ranging/consolidating markets

**Setup — Triple Confluence:**
- Context: HVN cluster aligns with previous day's POC AND a Fibonacci level (61.8%)
- Entry: Rejection trade at triple confluence
- Stop: Beyond the cluster
- Target: Opposite VA boundary
- Win rate: Claimed 85%+ (practitioner)

**Implementation Notes:**
- Add `naked_vpocs: Vec<NakedVpoc>` to `LevelsPipeline`
  - Struct: `{ session_date: String, price: f64, created_at: f64 }`
  - On each trade, check if price crosses any naked VPOC → mark as filled
  - Persist across sessions via database
- Add `prior_pocs` tracking in `session_summaries` or a dedicated table
- Composite profiles (5-day, 10-day, 20-day) as a future extension

**Backtesting Hypotheses:**
> What percentage of naked VPOCs get filled within 1, 3, 5, and 10 sessions?

> When price approaches a naked VPOC with confirming delta (session delta in approach direction), what is the bounce rate at the VPOC?

> What is the R-distribution when entering at a naked VPOC with a stop 10 NQ points beyond?
