# IDEA-019: Adaptive Session-Pace Volume Bars (Sierra Chart ACSIL Study)

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Idea
**Source:** Sierra Chart ACSIL custom chart bar docs; Relative Volume / cumulative volume ratio docs; April 2026 research pass
**Complements:** IDEA-006; discretionary execution chart design; session-awareness work

**Concept:** Build a Sierra Chart ACSIL custom chart bar study that adapts `contracts_per_bar` through the session instead of using a fixed N-volume threshold. The bar size should be smaller during quiet periods (for example Asia / slow Globex), then scale up automatically as expected participation rises into London, premarket, and RTH.

**Recommended metric:** Use **expected volume pace at this exact time of day**, then modulate it by **how fast today's session is running versus normal**.

- `expected_volume_per_minute(t)` = median 1-minute volume at the same clock time over the last 15-20 matching sessions
- `today_pace_adjustment(t)` = current cumulative volume to time `t` / average cumulative volume to time `t`
- `adaptive_contracts_per_bar(t)` = `expected_volume_per_minute(t) * today_pace_adjustment(t) / target_bars_per_minute`
- Prefer **median** over mean for the base curve so FOMC / earnings / macro spikes do not distort the threshold as badly

**Why this is preferable to a plain session average:**
- "Average volume so far this session" is too laggy and ignores the normal intraday volume curve
- NQ has distinct participation regimes across Asia, London, premarket, RTH open, lunch, and close
- Fixed volume bars still under-sample active periods and over-sample dead periods
- The actual goal is stable **visual density** on the execution chart, not a single static contracts-per-bar value

**Implementation direction:**
- Use an **ACSIL custom chart bar study** (`sc.UsesCustomChartBarFunction`) rather than an overlay-only study
- Drive the threshold calculation from a fixed-time reference chart (`30s` or `1m`), not from already-variable bars
- Sierra's built-in **Relative Volume** study is useful for prototyping same-time-of-day and cumulative-pace logic, but the final adaptive bars likely need a custom bar builder
- Smooth and clamp threshold changes so one anomalous minute does not radically change bar size
- Keep session templates explicit (RTH-only vs full Globex) and never mix scopes in the averaging logic

**Backtesting / validation questions:**
> Does an adaptive same-time-of-day volume threshold improve execution readability and signal timing versus fixed N-volume bars?

> Does `median same-time-of-day volume + cumulative pace ratio` outperform a plain rolling session-average threshold for bar construction?
