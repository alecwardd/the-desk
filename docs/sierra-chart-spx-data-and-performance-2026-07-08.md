# Sierra Chart — Live SPX Index Data & Performance Audit

**Date:** 2026-07-08 · **Status:** Point-in-time research note + same-day operator follow-up (no purchase/signup made; local Sierra performance settings and DLL loadout were changed) · **Author:** Claude Code (research task for Alec)

> **Where this lives and why.** This is not a trading-setup idea (no thesis/mechanics/regime/backtest verdict), so it does not belong in `docs/setup-ideas/` — that pipeline (see `docs/setup-ideas/index.md` and `_template.md`) is specifically for edge hypotheses that move Idea → Researched → Backtesting → Verdict. This is an infrastructure/ops research memo, closest in kind to `docs/phase-2-options-databento-memo.md` (a dated, point-in-time vendor/cost sketch). Placed directly in `docs/` for that reason. `docs/sierra-chart-settings.md` is the living settings reference this note feeds into, but that file was intentionally left untouched — the task asked for one net-new dated note, not edits elsewhere.

<!-- stats: point-in-time -->

---

## Part A — Live SPX index data in Sierra Chart

**One-line answer:** Yes — Sierra Chart can stream real-time SPX cash-index data (symbol **`SPX_CGI`**) via the **CBOE Global Indexes** feed inside Sierra's own **Denali Exchange Data Feed**. Alec already has **Package 11 Integrated Advanced** and `Allow Support for Sierra Chart Data Feeds = Yes`, and as of 2026-07-09 `SPX_CGI` is operational in Sierra as a 30-minute chart with 22 days loaded. The correct architecture is Rithmic for trading plus Sierra/Denali/CBOE Global Indexes for SPX/VIX context.

### What's required, layer by layer

| Layer | Requirement | Source |
|---|---|---|
| Service package | Must be an **Integrated** package (Package 10 Standard $36/mo, Package 11 Advanced $46/mo, or Package 12 Advanced+MBO $56/mo) — Integrated packages are the ones that support the Denali Exchange Data Feed **alongside** an external Data/Trading service like Rithmic. Base ("Sierra Chart data/services only") packages 3 ($26/mo) and 5 ($36/mo) do not combine with an external service the same way. | [Packages & Pricing](https://www.sierrachart.com/index.php?page=doc/Packages.php) — accessed 2026-07-08 |
| Data feed | **Denali Exchange Data Feed**, exchange = **CBOE Global Indexes** | [Denali Exchange Data Feed](https://www.sierrachart.com/index.php?page=doc/DenaliExchangeDataFeed.php) — accessed 2026-07-08 |
| Symbol | **`SPX_CGI`** for real-time S&P 500 cash index (also carries the CBOE VIX-family index). Historical daily data back to 1980-01-02; historical **intraday** only back to 2023-06-01 (feed is young). | [Denali Exchange Data Feed](https://www.sierrachart.com/index.php?page=doc/DenaliExchangeDataFeed.php); [Support Board #93150 "New CBOE SPX feed"](https://www.sierrachart.com/SupportBoard.php?ThreadID=93150); [Support Board #83679 "SPX Cash Index Feed"](https://www.sierrachart.com/SupportBoard.php?ThreadID=83679) — all accessed 2026-07-08 |
| Exchange fee | Denali's published range across all exchanges is **$2–$145/mo** depending on exchange and professional/non-professional status. One user report in the #93150 thread (2024) cited **~$6/mo** specifically for the CBOE Global Indexes add-on. **`verify live`** — Sierra's own account control panel is the authoritative number for Alec's account; the $6 figure is a 2024 forum report, not an official rate card line, so treat it as directional. | [Denali Exchange Data Feed](https://www.sierrachart.com/index.php?page=doc/DenaliExchangeDataFeed.php); [Support Board #93150](https://www.sierrachart.com/SupportBoard.php?ThreadID=93150) — accessed 2026-07-08 |
| Agreements | Uniform Subscriber Agreement + Non-Professional Certification must be signed in the account control panel before the exchange can be activated. | [Denali Exchange Data Feed](https://www.sierrachart.com/index.php?page=doc/DenaliExchangeDataFeed.php) — accessed 2026-07-08 |

**Net cost estimate (unconfirmed, `verify live`):** Alec is already on an Integrated package, so the incremental cost to add live SPX should be only the CBOE Global Indexes exchange fee (~$2–$10/mo most plausible band, per the one forum data point), not a service-package upgrade. The Sierra control panel showed a renewal warning for the current package, so services balance still needs attention before the package end date.

### Known rough edges (per the vendor's own support board, 2024 threads — may be stale by now)
- At launch, intraday historical backfill for `SPX_CGI` was thin (~1 month) and Sierra engineering called full backfill "several weeks away." Only ~2 years of intraday history exists as of this note (since 2023-06-01) — fine for live-alongside-ES use, not for deep historical SPX research.
- An older symbol, **`$INX`**, has much deeper daily history but **no real-time feed** — delayed/historical only, not a live-alongside-ES answer.
- These are 2024-era community reports; no evidence checked here confirms current (2026) data quality — flagged as `verify live`.

### Cheaper / delayed alternatives worth naming (not deeply evaluated — out of scope of this pass)
- **Delayed CBOE data**: Sierra's `RealtimeData.php` doc distinguishes delayed exchange data (bundled at no extra cost) from real-time; a delayed SPX print might already be reachable at zero incremental cost if `SPX_CGI` or an equivalent delayed symbol is exposed without the real-time exchange activation. Not verified — would need a live Sierra session to check Find Symbol.
- **Static/periodic ES↔SPX conversion**: rather than a live feed, a spreadsheet study or ACSIL study could track the ES-SPX fair-value spread once or twice a session (dividends + interest carry) and apply it as an offset — zero cost, but reintroduces the manual-conversion error the whole request is trying to eliminate, so treat as a fallback only.
- **This repo already ran a related but distinct vendor survey**: [`docs/setup-ideas/IDEA-027-options-data-vendor-comparison.md`](setup-ideas/IDEA-027-options-data-vendor-comparison.md) (2026-07-08, same day) compares API-accessible SPX **dealer-flow/GEX** vendors (cvforge, Unusual Whales, GEXBot, etc.) — that's a different problem (options positioning data for a trading edge, consumed by the-desk's Rust/MCP layer) from this one (a live SPX **spot price** shown on-screen next to ES for Alec's own reading). Worth being aware both tracks exist so a single vendor choice doesn't get asked to solve two different needs.

### Local evidence found on this machine
`T:\SierraChart\Data\SPXOptions.scid` exists but is **56 bytes** — Sierra's minimal empty-file header size, meaning **zero ticks have ever been recorded** for whatever symbol was configured under that chart. This is read-only-observed, not opened or modified. It suggests Alec (or a past session) already tried to set up an SPX-adjacent symbol and it never received data — consistent with the symbol never being found/activated rather than a live feed silently failing. Worth checking in Sierra's **File → Find Symbol** what that chart is actually pointed at.

### Same-day SPX / Denali follow-up (2026-07-08 night)

Screenshots and Sierra logs changed the unknowns:

- **Confirmed:** Sierra package is **11 - Integrated Advanced** and active through **2026-07-18 23:59:59 UTC**. The later **2026-07-23** date shown on the account page is the renewal-warning horizon, not the active service end date.
- **Confirmed:** Data/Trade Service remains **Rithmic Direct - DTC [trading]** and **Common Settings → Allow Support for Sierra Chart Data Feeds = Yes** is already set. This is the correct architecture for trading through Rithmic while receiving Sierra/Denali data for selected symbols.
- **Confirmed:** Denali Exchange Data Feed is active, and CBOE Global Indexes agreements are checked off.
- **Still open:** The control panel still showed `[Todo] Activate / Deactivate Denali Exchange Data Feed Exchanges [CBOE Global Indexes Exchange]`. That likely explains why `SPX_CGI` requests returned no usable intraday records.
- **Log evidence:** `SPX_CGI` was repeatedly requested through `cboe_global_indexes.data` but returned **0 intraday records**; `$INX` returned only a stale/delayed record and is not the live-SPX answer.
- **Interpretation:** the SPX problem is now narrowed to the CBOE Global Indexes entitlement/activation path. It should be handled separately from Sierra rendering/performance tuning.

### SPX operational result (2026-07-09)

Alec resolved the Sierra-side setup and now has a working **`SPX_CGI`**
chart: 30-minute candlestick bars with the last 22 days loaded. This
confirms the correct architecture was Rithmic for trading plus
Sierra/Denali/CBOE Global Indexes for the SPX cash index. The earlier
stall/zero-record behavior should be treated as an activation/symbol
setup issue, not evidence that the Rithmic + Denali hybrid configuration
is wrong.

Current SPX status: **operational for charting**. Remaining monitoring
is performance-related only: watch the current OpenGL/DLL-cleaned setup
through Globex and RTH before making more tuning changes.

---

## Part B — Sierra Chart performance audit (this machine, read-only)

### Evidence checked
- System hardware: CPU, RAM, GPU, disks, network adapters (system commands, this session)
- Sierra Chart install located at `T:\SierraChart` (not the assumed `C:\SierraChart` — `C:\SierraChart` does not exist on this machine)
- `T:\SierraChart\Data\*.scid` directory listing (file sizes/dates only — confirms multi-instrument recording described in `docs/multi-instrument-flow-architecture.md` is active: `ESU6`, `MNQU6`, `NQU6` all show `.scid` growth timestamped today)
- Running `SierraChart_64.exe` process: priority class, thread count, handle count, memory footprint
- `T:\SierraChart\ACS_Source\` — custom study source list and the repo's own compile script (`VisualCCompile.Bat`) to check build flags
- `T:\SierraChart\Data\*_64.dll` — compiled custom + third-party study DLLs present
- This repo's own `docs/sierra-chart-settings.md` (already-verified settings reference, last verified 2026-02-26/updated 2026-06-25)
- Sierra Chart's official docs: General Settings, Graphics Settings, Help #4 ("Prices/Data Falling Behind" — matches Alec's exact "stuck then catches up" symptom), Help #30 ("High CPU Usage/Long Time to Load"), Denali feed doc
- **Not parsed from disk**: `Sierra4.config` (Sierra's global-settings file) is a proprietary binary blob (`SCFG` header), not human-readable, and — per this task's privacy rule — it sits in the same file as account/position state, so it was not parsed or excerpted. Settings recorded below come from Alec's live UI screenshots/actions and process/module checks, not from reverse-engineering the config file.

### Verified facts (this machine)

| Item | Value |
|---|---|
| CPU | Intel Core i7-12700KF, 12 cores / 20 threads, 3.61 GHz base per WMI (this SKU's published boost is materially higher single-core; not independently measured here) |
| RAM | 32 GB total, ~10.4 GB available at time of check (system was otherwise in normal use) |
| GPU | NVIDIA GeForce RTX 3070 Ti (dedicated) + a "Microsoft Remote Display Adapter" is also present, meaning RDP is enabled on this box — worth knowing if Sierra is ever driven over a remote session, since OpenGL/GPU accel and RDP interact poorly (see below) |
| Sierra data drive (`T:`) | Sabrent Rocket 4.0 1TB, **NVMe SSD** — already matches Sierra's own #1 hardware recommendation |
| OS drive (`C:`) | Samsung 970 EVO Plus 500GB, **NVMe SSD** |
| Archive drive (`X:`) | External USB HDD (2TB) — used for cold storage per `docs/ops/automation-and-storage.md`, not in Sierra's live path, so not a latency factor |
| Network | Realtek Gaming 2.5GbE adapter, negotiated at 1 Gbps; a Tailscale VPN interface is also active on the box |
| Sierra process | `SierraChart_64.exe`, **Normal** priority class. Post-cleanup/restart sample: ~673 MB working set, ~1.35 GB private committed memory, 770 handles, 44 threads. |
| Custom study build flags | `VisualCCompile.Bat` in `ACS_Source` compiles with `/O2 /GL /Oi /Ot /Gy /D NDEBUG` — this **is already a Release-optimized build**, not a debug build. Recompiling existing custom studies is unlikely to buy anything; the flags are already right. |
| Custom + third-party studies loaded | After cleanup/restart, the only custom study DLL actually loaded in the Sierra process was `OrderFlowLabs.com.free_64.dll`. On disk, the active keep-set is now AMT Toolkit, Delta Dynamics loader, OrderFlowLabs FREE, and User Contributed Studies. |

### Same-day performance actions taken (2026-07-08 night)

- **OpenGL enabled:** Alec enabled **Use OpenGL for Chart Graphics** and restarted Sierra. The process loaded `OPENGL32.dll` plus NVIDIA's OpenGL driver (`nvoglv64.dll`), confirming OpenGL is active. Visuals looked fine during initial testing.
- **GPU health snapshot:** RTX 3070 Ti was visible and healthy in Windows/NVIDIA tooling. Post-restart sample: ~46 C, ~34% GPU utilization, ~2.4 GB / 8 GB VRAM used. No GPU-health concern surfaced.
- **Unused study DLLs quarantined:** Unused third-party/custom DLLs were moved out of active Sierra `Data` folders into `T:\SierraChart\Data\disabled-studies-2026-07-08`. This was intentionally reversible, not a permanent delete.
- **DLLs removed from active loadout:** Daily Market Generated Information, Delta Map, Delta Neutral Pivot Session, Dominance Detector, Hello World test, Inventory Value Area, Opening Range, OrderFlowLabs unsuffixed/beta/autoplot variants, Tick Imbalance Bars, weighted ATR, and duplicate secondary-instance OFL variants.
- **DLLs intentionally kept active on disk:** AMT Toolkit, Delta Dynamics loader (`DDY_Loader (1).dll`), OrderFlowLabs FREE, and User Contributed Studies.
- **Restart verified:** Before restart, old DLLs were still resident in Sierra memory. After full restart, only `OrderFlowLabs.com.free_64.dll` was loaded from the custom study set.
- **Measured baseline after restart:** working memory was roughly 80-130 MB lower than the prior running state, thread count dropped from 46 to 44, and the loaded module list was much cleaner. This is a modest but real baseline improvement; the real test is RTH/active Globex load.
- **Chart update interval strategy:** Global chart update interval was left conservative. Alec instead lowered update intervals only on DOMs/execution charts, which is the preferred approach: fast where interaction matters, slower for context charts.
- **Priority/threading decision:** Sierra stayed at Normal priority. Realtime priority is not recommended; Above Normal can be considered only after RTH evidence shows Windows scheduling starvation, which is not currently proven.

### Ranked recommendations

1. **(Next live test) Monitor the current setup through Globex and then RTH.** OpenGL is on, unused DLLs are out of the active loadout, and the DOM/execution charts now have lower per-chart update intervals. Do not make more performance changes until the system has been observed under a real fast tape.

2. **(Done, monitor) Global Settings → Graphics Settings → Use OpenGL for Chart Graphics.** Enabled and verified loaded through the NVIDIA OpenGL driver after restart. Keep it on as long as visuals remain correct and input/crosshair behavior stays normal.

3. **(Done in the right shape, monitor) Chart Update Interval.** Global value was conservative; Alec lowered update intervals only on DOMs/execution charts. Keep context charts slower. Do not push globally below Sierra's 100ms floor.

4. **(Conditional next performance lever) Sub-instances to use more of the 12-core/20-thread CPU.** Sierra's own docs and support board describe the core engine as effectively single-threaded per instance for study calculation; multiple instances (separate OS processes) are the documented way to spread footprint/DOM/heatmap-style heavy charts across this CPU's cores. This remains the next serious lever if Globex/RTH shows a local chart/study bottleneck after the OpenGL + DLL cleanup.

5. **(Leave alone for now) Process priority / CPU affinity.** Sierra Chart is currently running at **Normal** priority. Do not use Realtime priority. Above Normal can be tested later only if there is measured scheduler starvation, not just chart lag.

6. **(Repo-conflict — flag explicitly, do not follow this one vendor tip) Sierra's own Help #30 doc lists "increase Intraday Data Storage Time Unit from 1 Tick to 1 Second" as a CPU-reduction technique.** This directly conflicts with this repo's own mandatory setting: `docs/sierra-chart-settings.md` requires **1 Tick** storage on all four recorded charts (NQ/MNQ/ES/MES) because The Desk's delta/footprint/tape-pace pipelines depend on individual-trade granularity — going coarser would silently break those pipelines. **Do not apply this particular vendor performance tip.** If chart-rendering CPU load needs to come down, do it via Chart Update Interval / OpenGL / sub-instances (items 2–4 above), not via the storage time unit.

7. **(Vendor claim, low priority for this setup) Cloud/VPS.** Trading-VPS vendor marketing (TradingFXVPS, QuantVPS, etc. — treat these as vendor claims, not neutral sources) claims sub-2ms latency to CME from Chicago-colocated VPS versus general cloud. This is a genuine consideration for a **scalping/automated** setup trading every few seconds, but Alec is a discretionary trader reading structure, not latency-arbitraging — Sierra's own docs note general cloud/VPS mainly matters for automated high-frequency use, and a home NVMe workstation with a wired 1Gbps connection is very unlikely to be the actual cause of a "stuck then catches up" visual stall (that symptom looks like local rendering/update-interval or feed-side hiccups, not last-mile network throughput). **Recommendation: don't pursue a VPS/cloud migration based on current evidence** — revisit only if the Help #4 diagnostic in (1) points at network/feed lag specifically, and even then, investigate the existing wired connection and Rithmic feed health first.

### What could not be assessed here, and why
- **Complete Global Settings export** — `Sierra4.config` is a proprietary binary format, not text/JSON, and it's colocated with account data, so it was not parsed. Known live values/settings from Alec's screenshots and actions are recorded above, but the full settings state remains live-UI-only.
- **Which studies are actually attached to which charts, and their per-study calculation time** — Sierra exposes this live via the Chart Studies window ("study calculation times"); nothing on disk enumerates *active* studies per chart, only the list of compiled DLLs, which includes studies that may not be in use.
- **Whether removed study DLLs were ever attached to active charts** — after restart they no longer load, but this note did not audit every chart's Study list. If a chart references a removed DLL, Sierra may show a missing-study warning; restore from `T:\SierraChart\Data\disabled-studies-2026-07-08` if needed.
- **Real network latency/packet loss to Rithmic during trading hours** — this note was produced outside RTH with no live tick stream to measure against; Sierra's own diagnostic (Help #4) requires doing this live.
- **Live CBOE Global Indexes activation fee** — package tier is known (Package 11 Integrated Advanced), and `SPX_CGI` is now operational. The exact current monthly fee was not recorded in this note.

---

## Open questions for Alec

1. During Globex/RTH monitoring, note whether any "stuck then catches up"
   stall happens on one specific heavy chart (footprint/DOM/volume-profile)
   or across all charts including a plain Quote Board. Sierra's own
   diagnostic hinges on this distinction (local CPU vs. feed/network).
2. Do you ever drive this Sierra Chart instance over Remote Desktop, given
   RDP is enabled on this machine? If so, OpenGL acceleration recommendations
   above need a caveat.
3. If any chart complains about a missing study after the DLL quarantine,
   decide whether to restore that specific DLL or replace the chart/study
   dependency.

---

## Sources (all accessed 2026-07-08)

- [Real-time Data — Sierra Chart](https://www.sierrachart.com/index.php?page=doc/RealtimeData.php)
- [Description of Service Packages and Pricing — Sierra Chart](https://www.sierrachart.com/index.php?page=doc/Packages.php)
- [Denali Exchange Data Feed — Sierra Chart](https://www.sierrachart.com/index.php?page=doc/DenaliExchangeDataFeed.php)
- [Support Board #93150 — "New CBOE SPX feed"](https://www.sierrachart.com/SupportBoard.php?ThreadID=93150)
- [Support Board #83679 — "SPX Cash Index Feed"](https://www.sierrachart.com/SupportBoard.php?ThreadID=83679)
- [General Settings — Sierra Chart](https://www.sierrachart.com/index.php?page=doc/GeneralSettings.html)
- [Graphics Settings — Sierra Chart](https://www.sierrachart.com/index.php?page=doc/GraphicsSettings.html)
- [Help topic 4: Prices / Data Falling Behind — Sierra Chart](https://www.sierrachart.com/index.php?page=doc/helpdetails4.html)
- [Help topic 30: High CPU Usage/Long Time to Load Chart Data — Sierra Chart](https://www.sierrachart.com/index.php?page=doc/helpdetails30.html)
- [Support Board #98576 — "Optimizing chart responsiveness"](https://www.sierrachart.com/SupportBoard.php?ThreadID=98576)
- [Support Board #77795 — "10x faster than 4 Instances!!!"](https://www.sierrachart.com/SupportBoard.php?ThreadID=77795)
- [boostyourcharts.com — "Speed Up Sierra Chart: 7 Performance Settings Guide"](https://boostyourcharts.com/speed-performance/) — third-party/vendor blog, not official Sierra Chart documentation; treated as a secondary source and cross-checked against official docs above
- Internal: `docs/sierra-chart-settings.md`, `docs/ops/automation-and-storage.md`, `docs/multi-instrument-flow-architecture.md`, `docs/setup-ideas/IDEA-027-options-data-vendor-comparison.md` (this repo)
