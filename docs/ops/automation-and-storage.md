# Automation and Storage Runbook

This runbook covers The Desk's local Windows ops automation: Sierra Chart lifecycle tasks, SQLite archival/pruning (raw ticks **and DOM depth**), database backups, low-disk alarms, and the one-time external-drive reclaim.

> **See also:** [System Data Flow](../architecture/data-flow.md) — how Sierra Chart, the MCP server, agents, and this maintenance tooling fit together (who writes what, what server startup/shutdown triggers, and the on/off automation question). **Long maintenance jobs (the one-time depth reclaim) should run as a Windows Scheduled Task or in your own terminal — NOT from inside an agent session**, because an agent session restart kills its child processes and relaunches the MCP server (the `data.db` writer), which then contends. `scripts\ops\Run-Depth-Reclaim-Task.ps1` is the autonomous worker for the one-time reclaim.

> **What actually consumes the disk:** the dominant table is **`depth_events`** (DOM depth ingested from Sierra `.depth` files), not `raw_ticks`. It reached 3.6 B rows / ~600 GB before any retention existed. `raw_ticks` is comparatively tiny (~1.75 M rows). The real reclaim and ongoing upkeep both hinge on **pruning `depth_events`** (`depth_retention_days`, default 7); the `.depth` files in `T:\SierraChart\Data\MarketDepthData` are the durable, re-ingestable source.

## Scheduled Tasks

All tasks are registered under `\TheDesk\` by `scripts\ops\Register-DeskTasks.ps1`.

| Task | Trigger ET | Trigger Central | Account | Behavior |
| --- | --- | --- | --- | --- |
| `Sierra Watchdog` | Logon and every 4 minutes | Logon and every 4 minutes | Interactive user | During Sun 18:00 ET through Fri 17:00 ET, starts Sierra if `SierraChart_64` is not running. It does not close Sierra during the daily 17:00-18:00 ET maintenance halt. |
| `Sierra Weekend Close` | Friday 17:10 ET | Friday 16:10 Central | Interactive user | Calls `CloseMainWindow()`, waits up to 60 seconds, then force-kills Sierra if it has not exited. |
| `Sierra Sunday Open` | Sunday 17:50 ET | Sunday 16:50 Central | Interactive user | Starts Sierra about 10 minutes before Globex opens. |
| `Weekly Storage Archive` | Saturday 10:00 ET | Saturday 09:00 Central | `SYSTEM`, highest privileges | Runs `the-desk-storage --maintain`: archives old `raw_ticks` to cold zst **and prunes `depth_events` older than `depth_retention_days`**. Aborts if `the-desk-mcp` is running. Does not run vacuum (deleted pages are reused; compact only when needed). |
| `T Drive Low Disk Alarm` | Every 30 minutes | Every 30 minutes | `SYSTEM`, highest privileges | Logs `T:` free space and alerts if free space is below 40 GB. |
| `Monthly Storage Compaction` | First registered Saturday cadence, 12:00 ET | 11:00 Central | `SYSTEM`, highest privileges | Disabled by default. If enabled, compacts only when SQLite freelist size is at least 50 GB. |

Register or refresh the tasks from an elevated PowerShell session:

```powershell
cd C:\the-desk
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ops\Register-DeskTasks.ps1
```

Use `-DryRun` to preview registration. Use `-EnableMonthlyCompaction` only after the one-time reclaim has succeeded and the archive drive is stable.

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
X:\TheDesk\logs\               # ops logs
```

`~\.the-desk\config.toml` should use:

```toml
[storage]
warm_retention_days = 30      # raw_ticks kept hot in SQLite
cold_archive_dir = "X:\\TheDesk\\archive"
auto_archive = true           # vestigial (runtime ignores it; the scheduled task is the real automation)
depth_retention_days = 7      # DOM depth_events kept hot; older pruned (re-ingestable from .depth)

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
4. Runs `the-desk-storage --maintain --cutoff <ET-derived cutoff>` — archives old `raw_ticks` **and prunes `depth_events`** to `depth_retention_days`. On a first run with years of accumulated DOM depth this is the slow step: a chunked, WAL-bounded delete of billions of rows that can take **several hours** (~150–200 K rows/s). It is safe to leave running; the WAL is checkpointed so it cannot fill T:.
5. Runs `the-desk-storage --compact-into X:\TheDesk\state\data_compacted.db` — only now does the file shrink (delete moves pages to the freelist; `VACUUM INTO` copies just the live rows).
6. Verifies the compacted copy: SQLite integrity, required tables, `session_summaries > 0`, row-count parity, and no `raw_ticks` older than the warm cutoff.
7. Re-checks that `the-desk-mcp` is stopped and `data.db` is unlocked immediately before swapping.
8. Copies the compacted DB to `T:` and verifies that copy before replacing the original when it fits; otherwise falls back to delete-then-move.
9. Runs `the-desk-storage --status` as a smoke test and logs before/after `T:` free space.

Logs are written to `X:\TheDesk\logs`; pre-format logs temporarily start under `%TEMP%\TheDesk\logs` and are copied to `X:` after the archive drive mounts.

## Recovery Story

The reclaim deletes old `raw_ticks` from SQLite only after monthly zstd archives are written and verified. Pruned `depth_events` are deleted outright (no zst archive) because the Sierra `.depth` files already hold the same data far more compactly. The computed/research tables stay in the compacted SQLite DB. If old data is needed again, recover it from:

- `X:\TheDesk\archive\raw_ticks_*.csv.zst` for archived raw-tick SQLite rows.
- Sierra Chart `.scid` files in `T:\SierraChart\Data` for raw-tick replay/backfill.
- Sierra Chart `.depth` files in `T:\SierraChart\Data\MarketDepthData` (~92 GB) to re-ingest pruned DOM `depth_events`.

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

Disable storage automation:

```powershell
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Weekly Storage Archive"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "T Drive Low Disk Alarm"
Disable-ScheduledTask -TaskPath "\TheDesk\" -TaskName "Monthly Storage Compaction"
```

## Manual Checks

Check archive/storage state:

```powershell
$env:USERPROFILE = "C:\Users\alecw"
C:\the-desk\target_alt\release\the-desk-storage.exe --status
```

Check current tasks:

```powershell
Get-ScheduledTask -TaskPath "\TheDesk\" | Select-Object TaskName, State
```

Check recent logs:

```powershell
Get-ChildItem X:\TheDesk\logs | Sort-Object LastWriteTime -Descending | Select-Object -First 10
```
