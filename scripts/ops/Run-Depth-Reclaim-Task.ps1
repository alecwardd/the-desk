# One-time autonomous depth reclaim worker.
#
# Runs the LONG, resume-fragile steps of the depth_events reclaim — the chunked
# prune and the VACUUM INTO compaction — as a single process. It is meant to be
# launched by a Windows Scheduled Task so it runs under the Task Scheduler service
# and survives Claude Code / Cursor session restarts (which kill child processes
# and relaunch the MCP server).
#
# It deliberately STOPS before the irreversible swap: it leaves a verified
# compacted DB at X:\TheDesk\state\data_compacted.db for a final manual swap
# (delete the 629 GB original, move the compacted copy onto T:). Both --prune-depth
# and --compact-into are idempotent/resumable, so re-running after an interruption
# is safe.
#
# For a clean, fast run, close Cursor and Claude Code first so the MCP server does
# not relaunch and contend for the database write lock.

$ErrorActionPreference = "Stop"
$exe       = "C:\the-desk\target_alt\release\the-desk-storage.exe"
$compacted = "X:\TheDesk\state\data_compacted.db"
$log       = "X:\TheDesk\logs\depth-reclaim-task.log"

New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
function Log([string]$m) { "[{0}] {1}" -f ([DateTime]::Now.ToString("s")), $m | Tee-Object -FilePath $log -Append }

# The storage binary resolves ~/.the-desk from USERPROFILE; pin it in case the task
# runs under an account whose profile differs.
$env:USERPROFILE = "C:\Users\alecw"

Log "=== depth reclaim worker start ==="
Log "stopping the-desk-mcp (the only data.db writer that contends)"
Get-Process the-desk-mcp -ErrorAction SilentlyContinue | Stop-Process -Force

Log "step 1/2: --prune-depth (resumes any prior partial prune)"
& $exe --prune-depth *>> $log
if ($LASTEXITCODE -ne 0) { Log "prune-depth FAILED exit $LASTEXITCODE; aborting before compaction."; exit $LASTEXITCODE }
Log "prune-depth complete"

if (Test-Path $compacted) { Log "removing stale compacted copy $compacted"; Remove-Item $compacted -Force }

Log "step 2/2: --compact-into $compacted (VACUUM INTO + verify)"
& $exe --compact-into $compacted *>> $log
if ($LASTEXITCODE -ne 0) { Log "compact-into FAILED exit $LASTEXITCODE."; exit $LASTEXITCODE }

Log "=== done. Verified compacted DB ready at $compacted ==="
Log "FINAL MANUAL STEP (quick, irreversible): delete T:\TheDesk\state\data.db (+ -wal/-shm), move the compacted copy to T:\TheDesk\state\data.db, then re-enable [backup] and restart the MCP."
