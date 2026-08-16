# Automation and Storage Runbook

This runbook covers The Desk's local Windows ops automation: Sierra Chart lifecycle tasks, SQLite archival/pruning (raw ticks **and DOM depth**), database backups, low-disk alarms, weekend readiness checks, and the one-time external-drive reclaim.

> **See also:** [System Data Flow](../architecture/data-flow.md) — how Sierra Chart, the MCP server, agents, and this maintenance tooling fit together (who writes what, what server startup/shutdown triggers, and the on/off automation question). **Long maintenance jobs (the one-time depth reclaim) should run as a Windows Scheduled Task or in your own terminal — NOT from inside an agent session**, because an agent session restart kills its child processes and relaunches the MCP server (the `data.db` writer), which then contends. `scripts\ops\Run-Depth-Reclaim-Task.ps1` is the autonomous worker for the one-time reclaim.

> **What actually consumes the disk:** historically the dominant table was **`depth_events`** (DOM depth copied from Sierra `.depth` files into SQLite). It reached 3.6 B rows / ~600 GB before any retention existed. **SIL-M3f stopped live bulk appends** — the ~1s `.depth` poll still reconstructs the book and writes compact `dom_snapshots` / `dom_feature_snapshots` plus pipeline `domSummary` (Journal Frames / Capsules). `raw_ticks` is comparatively tiny (~1.75 M rows). Existing DBs can still **prune leftover `depth_events`** (`depth_retention_days`, default 7) with `--prune-depth` / `--maintain`. The `.depth` files in `T:\SierraChart\Data\MarketDepthData` are the durable source. Weekend VACUUM / `--compact-into` reclaims freelist pages and is **ops-track** — not required to stop the writes.

## Timezone note

Scheduled task triggers use **machine local wall-clock** time. This trading workstation is expected to be set to **Central Time**. ET equivalents in the table below are CT+1 (standard offset used in descriptions; ignore DST edge cases for ops planning and prefer the local times on the box).

## Exit codes (ops scripts)

Shared convention (`scripts/ops/Desk-ExitCodes.ps1`):

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 2 | Deferred (MCP writer active / maintenance blocked) — **not** silent success |
| 3 | Config error (including `Register-DeskTasks.ps1 -Verify` mismatches) |
| 4 | Integrity failure (stale/missing maintenance marker, health criticals) |
| 5 | Storage failure (X: missing, free space critical, temp unusable) |
| 1 | General failure |

Weekend maintenance must not look green when it skipped: `Run-Weekly-Archive.ps1` exits **2** when `the-desk-mcp` is running, logs `DEFERRED`, and writes `X:\TheDesk\logs\last-deferred-maintenance.json`. On success it writes `X:\TheDesk\logs\last-successful-maintenance.json`.

## Scheduled Tasks

All tasks are registered under `\TheDesk\` by `scripts\ops\Register-DeskTasks.ps1`.

| Task | Trigger local (CT) | Trigger ET | Account | Behavior |
| --- | --- | --- | --- | --- |
| `Sierra Watchdog` | Logon and every 4 minutes | same | Interactive user | During Sun 18:00 ET through Fri 17:00 ET, starts Sierra if `SierraChart_64` is not running. It does not close Sierra during the daily 17:00-18:00 ET maintenance halt. |
| `Engine Watchdog` | Logon and every 4 minutes | same | Interactive user | SIL-M2a: starts `the-desk-engine` if down so ingest covers Globex overnight when `[sil].engine_mode=external`. See `docs/ops/engine-lifecycle.md`. |
| `Sierra Weekend Close` | Friday 16:10 | Friday 17:10 | Interactive user | Calls `CloseMainWindow()`, waits up to 60 seconds, then force-kills Sierra if it has not exited. |
| `Friday Data Readiness` | Friday 16:20 | Friday 17:20 | `SYSTEM` | Runs `Invoke-FridayDataReadiness.ps1`: Sierra/SCID idle check, `the-desk-storage --status`, prints operator-gated catch-up MCP/CLI commands, writes `weekend-readiness-YYYYMMDD.json`. Does **not** ingest/backfill unless the operator runs those commands. |
| `Weekly Storage Archive` | Saturday 09:00 + hourly for 10h | Saturday 10:00 + hourly | `SYSTEM`, highest privileges | Saturday storage maintenance (name retained). Runs `the-desk-storage --maintain`: archives old `raw_ticks` + prunes leftover `depth_events` on existing DBs. **Exit 2** if MCP is up (deferred marker). Success marker on completion. Passes `--abort-if-mcp` when the binary advertises it. Live ingest no longer grows `depth_events` (SIL-M3f). |
| `Sunday Pre-Open Readiness` | Sunday 16:40 | Sunday 17:40 | `SYSTEM` | `Invoke-DeskHealthCheck.ps1 -Mode SundayPreOpen` — fails if last successful maintenance marker is missing/stale before Globex. |
| `Sierra Sunday Open` | Sunday 16:50 | Sunday 17:50 | Interactive user | Starts Sierra about 10 minutes before Globex opens. |
| `Storage Health Check` | Every 6 hours | Every 6 hours | `SYSTEM` | `Invoke-DeskHealthCheck.ps1 -Mode Daily` — T:/X: free space, task presence, maintenance/backup age, optional storage `--status`. Logs to `X:\TheDesk\logs\health-*.log`. |
| `T Drive Low Disk Alarm` | Every 30 minutes | Every 30 minutes | `SYSTEM` | Logs `T:` (and `X:`) free space; alerts if `T:` &lt; 40 GB or `X:` &lt; 50 GB. SYSTEM `msg.exe` targets the interactive user via CIM when possible. |
| `Monthly Storage Compaction` | First registered Saturday cadence, 11:00 | 12:00 | `SYSTEM` | Disabled by default. If enabled, compacts only when SQLite freelist size is at least 50 GB. |

### Cadence (Fri → Sat → Sun → Daily)

1. **Friday 16:10** — Sierra close
2. **Friday 16:20** — data readiness manifest (no silent catch-up)
3. **Saturday 09:00–19:00** — storage archive retries until MCP is idle (exit 2 while deferred)
4. **Sunday 16:40** — pre-open health (maintenance must have succeeded)
5. **Sunday 16:50** — Sierra open
6. **Every 6 hours** — storage health check

Register or refresh the tasks from an elevated PowerShell session:

```powershell
cd C:\the-desk
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ops\Register-DeskTasks.ps1
```

Optional: pass a profile path explicitly (defaults to `$env:USERPROFILE`):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ops\Register-DeskTasks.ps1 -UserProfilePath $env:USERPROFILE
```

Use `-DryRun` to preview registration (no `Register-ScheduledTask`). Use `-Verify` to PASS/FAIL-check existing tasks (existence, enabled state, action executable/script args, working directory, principal, triggers, next run) without elevating:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ops\Register-DeskTasks.ps1 -DryRun
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ops\Register-DeskTasks.ps1 -Verify
```

Use `-EnableMonthlyCompaction` only after the one-time reclaim has succeeded and the archive drive is stable.

## Sierra Chart Operating Requirement

The Sierra tasks are interactive-session tasks so Sierra opens on the visible desktop. The Windows user must remain logged on; locked is fine, logged off is not. If Windows reboots to the login screen, the watchdog cannot launch Sierra into a non-existent desktop session.

For away-from-home reliability, enable Windows auto-logon for this trading workstation and use a UPS. Also set Sierra Chart Global Settings so `LightweightChartBook2026.Cht` opens on startup; the watchdog still relies on Sierra for chartbook restore, but the startup setting avoids reopening the wrong chartbook after an abnormal exit.

## Storage Layout

Expected layout after reclaim:

```text
T:\TheDesk\state\data.db       # hot SQLite DB on fast NVMe
X:\TheDesk\archive\            # zstd cold raw_tick archives
X:\TheDesk\state\              # reclaim scratch/compacted copy
X:\TheDesk\temp\               # SQLite temp files during maintenance
X:\TheDesk\logs\               # ops logs + maintenance/readiness markers
X:\TheDesk\backups\            # VACUUM INTO snapshots
```

Markers / manifests:

| File | Written by |
| --- | --- |
| `X:\TheDesk\logs\last-successful-maintenance.json` | `Run-Weekly-Archive.ps1` on success |
| `X:\TheDesk\logs\last-deferred-maintenance.json` | `Run-Weekly-Archive.ps1` when MCP blocks (exit 2) |
| `X:\TheDesk\logs\weekend-readiness-YYYYMMDD.json` | `Invoke-FridayDataReadiness.ps1` |
| `X:\TheDesk\logs\health-*.log` | `Invoke-DeskHealthCheck.ps1` |

`~\.the-desk\config.toml` should use:

```toml
[storage]
warm_retention_days = 30      # raw_ticks kept hot in SQLite
cold_archive_dir = "X:\\TheDesk\\archive"
auto_archive = true           # vestigial (runtime ignores it; the scheduled task is the real automation)
depth_retention_days = 7      # leftover depth_events prune window; live ingest no longer writes this table (SIL-M3f)

[backup]
enabled = true                # set false only during a one-time reclaim
directory = "X:\\TheDesk\\backups"   # on the 1.8 TB drive, NOT the near-full T:
min_interval_hours = 24
```

`auto_archive` is vestigial: the Rust/MCP runtime does not act on it. The scheduled `Weekly Storage Archive` task is the actual automation.

### Database backups — disk-fill hazard

The MCP server takes an automatic **`VACUUM INTO` snapshot on startup** (`[backup]`, default dir `~/.the-desk/backups` → T:). The snapshot is ~the full DB size, so on a near-full drive — or before the DB has been compacted — it can **fill the disk** (observed: a 67 GB partial snapshot took T: to 0 GB free, which would halt recording). Two safeguards:

- Point `[backup].directory` at **X:** so a full-size snapshot can never fill T:. Keep `enabled = false` only while a one-time reclaim runs, then re-enable once the DB is compacted (a snapshot is then small/fast).
- `perform_backup` deletes the partial file if `VACUUM INTO` fails, so a doomed backup can't accumulate as an orphan.

Note: the `the-desk-mcp` server is launched by **whatever Claude Code / Cursor session is active**, not only Cursor — so it can restart and re-trigger the startup backup. Stop it before any DB maintenance.

### On-demand verified backups (`--backup`)

Take a verified full backup from the CLI without the MCP server:

```powershell
C:\the-desk\target_alt\release\the-desk-storage.exe --backup --abort-if-mcp
```

`--backup` writes `desk-<UTC timestamp>.db` to the configured `[backup]` directory via the same
`perform_backup` path as the MCP startup snapshot: SQLite header magic + `PRAGMA quick_check` +
durable-table verification, read-only/immutable opens, orphan-journal cleanup, and retention
pruning that never deletes the newest header-valid backup. It ignores `[backup].enabled` and
`min_interval_hours` (those gate only the automatic startup path). It is a standalone mode: only
`--abort-if-mcp` may accompany it (anything else exits 3).

**Backups are not reclaim copies.** `--backup` deliberately **keeps unarchived history** — a
restore point containing rows older than the warm-retention cutoff is the safe direction.
`--compact-into` is the reclaim-swap tool and keeps its strict assertion that no `raw_ticks`
predate the cutoff (exit 4 otherwise), because a reclaim copy that still carries unarchived rows
means the archive step was skipped. Use `--backup` for restore points; use `--compact-into` only
inside the reclaim flow.

## Storage Binary Deployment

The scheduled tasks and ops scripts run `target_alt\release\the-desk-storage.exe` (built with
`CARGO_TARGET_DIR=target_alt` so storage builds never contend with the main `target\` dir). To
deploy a new build safely:

1. **Build from a committed ref, not the working tree** — a dirty tree leaks uncommitted library
   code into the binary. Use a detached worktree with a temporary target dir:

   ```powershell
   git -C C:\the-desk worktree add $env:TEMP\deploy-storage <commit>
   $env:CARGO_TARGET_DIR = "$env:TEMP\deploy-target"
   cargo build --manifest-path $env:TEMP\deploy-storage\Cargo.toml --release --bin the-desk-storage
   cargo test  --manifest-path $env:TEMP\deploy-storage\Cargo.toml --bin the-desk-storage
   ```

2. Record the candidate's SHA-256 (`Get-FileHash`).
3. Pick a window **off-market and at least 10–15 minutes clear of the six-hourly Storage Health
   Check marks** (00:03 / 06:03 / 12:03 / 18:03 CT); confirm no `the-desk-storage.exe` process
   is running.
4. Copy the current exe to a dated `.bak.exe` beside it, then copy the candidate over
   `the-desk-storage.exe`.
5. Re-hash the deployed file against the recorded SHA-256 and smoke-test: `--help` must exit 0,
   and a conflicting-mode call such as `--backup --status` must exit 3. Roll back from the
   `.bak.exe` on any mismatch.

## One-Time Reclaim Runbook

Do this from an elevated PowerShell session. The script has two destructive gates: formatting Disk 2 and replacing the original `data.db`. Both require explicit `-Confirm` and runtime verification.

1. Build the storage binary used by the scripts:

   ```powershell
   cd C:\the-desk
   $env:CARGO_TARGET_DIR = "target_alt"
   cargo build --release --bin the-desk-storage
   ```

2. Preview the full plan:

   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ops\Reclaim-Storage.ps1 -Mode FullReclaim -DryRun
   ```

3. Run the reclaim:

   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ops\Reclaim-Storage.ps1 -Mode FullReclaim -Confirm
   ```

The script verifies Disk 2 is the expendable Seagate USB drive before formatting. It refuses to format if the target is not Disk 2, is not `Seagate*`, is not USB, is outside the expected ~1.8 TB size, is boot/system, or has `C:`/`T:` assigned.

During DB work the script stops only `the-desk-mcp`. Sierra Chart may keep running because Sierra does not lock The Desk's SQLite DB. The script then:

1. Mounts `X:` as NTFS `DeskArchive`.
2. Moves existing cold archives from `T:\TheDesk\archive` to `X:\TheDesk\archive`.
3. Runs `the-desk-storage --status` to catch archive filename collisions.
4. Runs `the-desk-storage --maintain --cutoff <ET-derived cutoff>` — archives old `raw_ticks` **and prunes leftover `depth_events`** to `depth_retention_days` on existing DBs (SIL-M3f stopped live appends; this step is for rows already in the table). On a first run with years of accumulated DOM depth this is the slow step: a chunked, WAL-bounded delete of billions of rows that can take **several hours** (~150–200 K rows/s). It is safe to leave running; the WAL is checkpointed so it cannot fill T:. Reclaiming the freed pages (`--compact-into` / `--vacuum`) is a separate ops-track step — not required to stop live writes.
5. Runs `the-desk-storage --compact-into X:\TheDesk\state\data_compacted.db` — only now does the file shrink (delete moves pages to the freelist; `VACUUM INTO` copies just the live rows).
6. Verifies the compacted copy: SQLite integrity, required tables, `session_summaries > 0`, row-count parity, and no `raw_ticks` older than the warm cutoff.
7. Re-checks that `the-desk-mcp` is stopped and `data.db` is unlocked immediately before swapping.
8. Copies the compacted DB to `T:` and verifies that copy before replacing the original when it fits; otherwise falls back to delete-then-move.
9. Runs `the-desk-storage --status` as a smoke test and logs before/after `T:` free space.

Logs are written to `X:\TheDesk\logs`; pre-format logs temporarily start under `%TEMP%\TheDesk\logs` and are copied to `X:` after the archive drive mounts.

## Recovery Story

The reclaim deletes old `raw_ticks` from SQLite only after monthly zstd archives are written and verified. Leftover `depth_events` (from before SIL-M3f stopped live appends) are deleted outright (no zst archive) because the Sierra `.depth` files already hold the same data far more compactly. The computed/research tables stay in the compacted SQLite DB. If old data is needed again, recover it from:

- `X:\TheDesk\archive\raw_ticks_*.csv.zst` for archived raw-tick SQLite rows.
- Sierra Chart `.scid` files in `T:\SierraChart\Data` for raw-tick replay/backfill.
- Sierra Chart `.depth` files in `T:\SierraChart\Data\MarketDepthData` (~92 GB) to reconstruct DOM via `DepthReader` (do not re-ingest into `depth_events`; that table is no longer the hot store).

The Desk reads `.scid`/`.depth`; it does not alter Sierra's recording files.

The hot DB remains on `T:` after compaction. Do not run The Desk from the USB drive.

## Pausing Automation

Disable Sierra lifecycle tasks before maintenance windows that intentionally close Sierra:

```powershell
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Sierra Watchdog"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Sierra Weekend Close"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Sierra Sunday Open"
```

Re-enable them:

```powershell
Enable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Sierra Watchdog"
Enable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Sierra Weekend Close"
Enable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Sierra Sunday Open"
```

Disable storage / readiness automation:

```powershell
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Weekly Storage Archive"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Friday Data Readiness"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Sunday Pre-Open Readiness"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Storage Health Check"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "T Drive Low Disk Alarm"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Monthly Storage Compaction"
```

## Manual Checks

Check archive/storage state:

```powershell
$env:USERPROFILE = $env:USERPROFILE   # or an explicit profile path
C:\the-desk\target_alt\release\the-desk-storage.exe --status
```

Check current tasks / verify registration:

```powershell
Get-ScheduledTask -TaskPath "\TheDesk\" | Select-Object TaskName, State
powershell -NoProfile -ExecutionPolicy Bypass -File C:\the-desk\scripts\ops\Register-DeskTasks.ps1 -Verify
```

> **Scheduler visibility gotcha:** the storage/readiness tasks run as `SYSTEM`, and
> **non-elevated folder listings silently omit tasks the caller cannot read** — from a normal
> session, `Get-ScheduledTask -TaskPath "\TheDesk\"` shows only the interactive Sierra tasks,
> which looks exactly like "tasks were never registered." To check registration without
> elevating, query each task **by name**: `schtasks /Query /TN "\TheDesk\Storage Health Check"`
> returning `Access is denied` proves the task **exists**; only "cannot find" means absent.
> Alternatively, fresh `health-Daily-*.log` files under `X:\TheDesk\logs` (written every 6 hours
> at :03) prove the health task is live. Known defect: non-elevated `-Verify` currently labels
> access-denied tasks as missing — run `-Verify` elevated until that is fixed.

Run a health check:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File C:\the-desk\scripts\ops\Invoke-DeskHealthCheck.ps1 -Mode Daily
```

Check recent logs:

```powershell
Get-ChildItem X:\TheDesk\logs | Sort-Object LastWriteTime -Descending | Select-Object -First 10
```
