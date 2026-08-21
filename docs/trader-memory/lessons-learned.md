# Lessons Learned

This is the hand-edited long-term lesson log. Add dated entries when a session, review, or repeated pattern deserves to become durable doctrine.

## Template

```text
YYYY-MM-DD — Lesson title
Context:
What happened:
What The Desk should remember:
Promote to doctrine? yes/no
```

## Seed Lessons

2026-05-01 — Memory must know the trader, not just the market
Context: The Desk already exposes rich market structure, but agents need persistent trader-specific context.
What happened: The memory layer was expanded to combine a trader profile, current-context edge brief, analog sessions, and curated markdown doctrine.
What The Desk should remember: Every session-start, setup, and risk discussion should include both market context and trader-specific behavioral/performance context when available.
Promote to doctrine? yes

## Lessons

> **Evidence base.** Entries dated 2026-08-06 derive from a fill-level study of one
> month of recorded Sierra fills (2026-07-07 to 2026-08-06, 1,615 FIFO round trips)
> plus a 57.5M-tick MNQ replay used to compute true MFE/MAE. Absolute account P&L,
> account identifiers, broker threshold fields, and per-account balances stay in the
> private risk ledger and are deliberately not reproduced here. Figures below are
> expressed in NQ points, ratios, percentages, and win rates so the doctrine is
> portable and reviewable without exposing account state.

2026-08-06 — Stops belong in fixed points; volatility belongs in the filter, not the bracket
Context: 57.5M MNQ ticks replayed against 1,220 matched round trips to compute true MFE/MAE. Replay is bounded by the trader's actual exit, so large-target results are conservative.
What happened: Funded winner MAE was 4.38 / 5.00 / 5.50 points across LOW / MID / HIGH volatility regimes while ATR(5m,14) went 22.28 / 36.53 / 58.95 — heat taken barely moved as volatility nearly tripled. Coefficient of variation for winner MAE was 1.19 in points versus 1.22 in ATR units, so ATR normalization did not stabilize risk. MFE scaled only weakly (18.38 → 24.75 points). In bracket replay a FIXED 10/24 bracket ranked best on funded accounts, with both ATR-scaled variants (1.0x/2.0x and 1.25x/2.5x) below it. On evaluation accounts the ranking inverted and ATR 1.25x/2.5x ranked first, because those trades were held long enough for large targets to resolve.
What The Desk should remember: Adverse excursion is a function of entry technique, not volatility — do not scale stops by ATR for this trader. Scale targets coarsely at most (one widening above ATR 48). Use volatility as a participation filter instead: funded accounts were materially negative in the lowest volatility tercile and near flat in the highest. Before recommending ATR-proportional brackets for anyone, test whether MAE actually scales with ATR in their own fills.
Promote to doctrine? yes

2026-08-06 — Quantify giveback with MFE-of-losers before prescribing a breakeven rule
Context: Same tick study. The trader reported that losses felt like "letting profits come back to me."
What happened: 58.9% of funded losing trades reached at least +4 points, 44.7% reached +6, and 30.6% reached +10 before ending red. Median funded loser peaked at +5.00 points. Separately, median funded winner had 21.25 points of MFE available and captured 10.00 — a 0.527 capture efficiency against 0.679 on evaluation accounts, with nearly identical MFE available (21.25 vs 25.25).
What The Desk should remember: The trader's intuition was correct and is now sized: a breakeven stop at +6 points neutralizes roughly 45% of funded losing trades. Report MFE-of-losers and capture efficiency alongside win rate whenever a trader describes giveback. Equal MFE availability across account phases is strong evidence that a performance gap is behavioral rather than setup- or regime-driven.
Promote to doctrine? yes

2026-08-06 — The edge survives the conversion; the trader does not
Context: One month of Sierra fills, 2026-07-07 to 2026-08-06, 1,615 FIFO round trips across the active account set, reconciled against the firms' dashboards.
What happened: Evaluation-phase accounts were net positive (537 trades, 59% WR, 1.03 win/loss ratio, 5m57s median hold). Funded-phase accounts were net negative (1,078 trades, 52% WR, 0.77 ratio, 2m40s median hold). The gap is size-independent in NQ points: evaluation winners captured 23.18 points versus 14.87 funded, while average loss was nearly unchanged (−22.24 vs −17.91). One account tracked across both phases went from 64% WR / 7m07s / median 3.5 contracts as an evaluation to 34% WR / 2m01s / median 5.0 contracts after conversion.
What The Desk should remember: Account phase is a first-class risk variable. When a trader converts an evaluation to funded, expect winners to be cut short and position size to drift up, and set server-side Max Position at or below the size that passed the evaluation. Never infer a broken edge from funded-account results without checking the eval-phase baseline in points.
Promote to doctrine? yes

2026-08-06 — Session time is the largest single leak on funded accounts
Context: Same dataset, split by hour in America/Chicago.
What happened: Funded accounts were net negative across 430 overnight trades (00:00–07:59, 46.3% WR) and roughly flat across 611 RTH trades (08:00–15:59, 55.8% WR). The pattern held on 4 of 4 funded accounts with overnight activity. Evaluation accounts were net positive overnight, almost entirely in the 05:00 hour.
What The Desk should remember: Session-time performance must be evaluated per account phase, not pooled. Do not generalize an eval-phase overnight result to funded accounts.
Promote to doctrine? yes

2026-08-06 — Hold-time asymmetry, not win rate, is the failure mode
Context: Funded-account hold-time buckets from the same dataset.
What happened: The 30–60s and 1–2m buckets were both net positive (~62% WR). The 10–20m bucket was the largest negative bucket at a 32% win rate. Sub-30s was negative at 43%. Win rates of 55–80% still produced losing days whenever average win fell below average loss.
What The Desk should remember: Report average-win-to-average-loss ratio and the winner-vs-loser hold-time ratio alongside win rate. A win rate above 55% with a ratio below 1.0 is a losing configuration and should be flagged as such.
Promote to doctrine? yes

2026-08-06 — Verify aggregate-derived rules against fill-level data before promoting them
Context: Two rules proposed the same day from prop-dashboard daily aggregates.
What happened: A "cap 6 round trips per day" rule was contradicted — account-days with 41+ trades were net positive (58% WR) while 1–3 trade days were net negative (18% WR). An "avoid Wednesdays" rule was an artifact of a single session: net negative in aggregate, but net positive once 2026-08-05 is excluded.
What The Desk should remember: Daily aggregates hide reverse causality — trade count is an outcome of a day going well, not a cause. Never promote a behavioral rule from dashboard-level daily summaries without a fill-level check and a single-day-removal sensitivity test. Record withdrawn rules rather than deleting them.
Promote to doctrine? yes

2026-08-06 — Separate manual execution decorrelates fills, not days
Context: Trader deliberately traded accounts separately rather than using a copier, believing it spread risk.
What happened: Of 2,615 cross-account trade pairs, only 1.5% entered within 10 seconds of each other (3.9% within 60s) — entries were genuinely independent. Yet daily outcome sign agreement was 79–80% across all three funded pairs, with correlation 0.32–0.52. Days on which the full account set traded averaged 196 trades and were net negative.
What The Desk should remember: Multi-account diversification should be measured on daily outcome correlation, not entry timing. When the driver is trader state and regime, separate execution multiplies attention cost without buying meaningful decorrelation.
Promote to doctrine? yes

2026-08-06 — On EOD-trailing accounts, a floor that never rises is a countdown timer
Context: Postmortem of a breach on an end-of-day-trailing funded account.
What happened: The account's EOD high-water mark was set on day one, locking the max-loss floor just below it. It then traded 22 sessions without a single new closing high and ground through the entire cushion, breaching by a negligible margin.
What The Desk should remember: For EOD-trailing products, track consecutive sessions without a new closing high as a risk metric. A modest green close that sets a new high-water mark is worth more than a large day that round-trips to flat.
Promote to doctrine? yes

2026-08-06 — Intraday-trail products penalize unrealized giveback permanently
Context: An intraday-trailing funded account, with the broker's auto-liquidate threshold field read directly rather than inferred from the product's advertised drawdown.
What happened: The threshold implied a peak equity above the session's starting balance. Roughly 60% of one daily loss limit in unrealized profit was given back, permanently raising the floor by that amount, before the session closed red and auto-liquidated on the firm's daily loss limit.
What The Desk should remember: On intraday-trailing accounts, open profit that round-trips is a permanent cushion loss, not a neutral event. Bank scale-outs rather than pressing runners, and read the broker's threshold field directly rather than assuming the product's advertised starting drawdown.
Promote to doctrine? yes
