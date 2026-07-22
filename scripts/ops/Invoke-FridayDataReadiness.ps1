<#
.SYNOPSIS
  Friday post-close data readiness check (non-destructive by default).

.NOTES
  Exit codes (see Desk-ExitCodes.ps1):
    0 success
    2 deferred / refused (MCP writer active when catch-up requested)
    3 config error
    4 integrity failure
    5 storage failure
    1 general failure

  Catch-up (ingest/backfill) is operator-gated:
    - Without -ConfirmCatchUp the script only prints the exact MCP/CLI commands.
    - With -ConfirmCatchUp it still refuses if the-desk-mcp is running (exit 2).

  Triggers use machine local wall-clock. This workstation is expected to be Central Time
  (Friday 16:20 local ≈ 17:20 ET, after Sierra close at 16:10 CT / 17:10 ET).
#>
param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$UserProfilePath = $env:USERPROFILE,
    [string]$SierraDataDir = "",
    [int]$ScidIdleSeconds = 120,
    [switch]$ConfirmCatchUp,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "Desk-ExitCodes.ps1")

$script:LogPath = $null
$script:StorageExe = Join-Path $RepoRoot "target_alt\release\the-desk-storage.exe"
$script:ConfigPath = Join-Path $UserProfilePath ".the-desk\config.toml"
$script:McpProcessName = "the-desk-mcp"

function Initialize-Logging {
    $logDir = if (Test-Path "X:\") { "X:\TheDesk\logs" } elseif ($DryRun) {
        Join-Path $UserProfilePath ".the-desk\logs"
    } else {
        Join-Path $env:TEMP "TheDesk\logs"
    }
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $script:LogPath = Join-Path $logDir ("friday-readiness-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) { Initialize-Logging }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $script:LogPath -Value $line
    Write-Host $line
}

function Get-TomlHint {
    param([Parameter(Mandatory)][string]$Key)
    if (-not (Test-Path -LiteralPath $script:ConfigPath)) {
        return $null
    }
    $line = Select-String -LiteralPath $script:ConfigPath -Pattern ("^\s*{0}\s*=" -f [regex]::Escape($Key)) |
        Select-Object -First 1
    if (-not $line) { return $null }
    if ($line.Line -match '=\s*"([^"]+)"') { return $Matches[1] }
    if ($line.Line -match "=\s*'([^']+)'") { return $Matches[1] }
    if ($line.Line -match "=\s*(\S+)") { return $Matches[1].Trim() }
    return $null
}

function Get-DriveFreeGb {
    param([Parameter(Mandatory)][string]$Letter)
    try {
        $vol = Get-Volume -DriveLetter $Letter -ErrorAction Stop
        return [math]::Round($vol.SizeRemaining / 1GB, 2)
    } catch {
        return $null
    }
}

function Test-SierraRecordingIdle {
    $sierraRunning = [bool](Get-Process -Name "SierraChart_64" -ErrorAction SilentlyContinue)
    $dataDir = $SierraDataDir
    if ([string]::IsNullOrWhiteSpace($dataDir)) {
        $dataDir = Get-TomlHint -Key "sierra_data_dir"
    }
    if ([string]::IsNullOrWhiteSpace($dataDir)) {
        $dataDir = "T:\SierraChart\Data"
    }

    $result = [ordered]@{
        sierraRunning     = $sierraRunning
        sierraDataDir     = $dataDir
        newestScid        = $null
        newestScidAgeSec  = $null
        scidAppearsIdle   = $null
        note              = $null
    }

    if (-not (Test-Path -LiteralPath $dataDir)) {
        $result.note = "Sierra data dir not found: $dataDir"
        return [pscustomobject]$result
    }

    $newest = Get-ChildItem -LiteralPath $dataDir -Filter "*.scid" -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (-not $newest) {
        $result.note = "No .scid files under $dataDir"
        return [pscustomobject]$result
    }

    $ageSec = [math]::Round(((Get-Date) - $newest.LastWriteTime).TotalSeconds, 0)
    $result.newestScid = $newest.FullName
    $result.newestScidAgeSec = $ageSec
    $result.scidAppearsIdle = ($ageSec -ge $ScidIdleSeconds)

    if ($sierraRunning -and -not $result.scidAppearsIdle) {
        $result.note = "Sierra still running and newest SCID modified ${ageSec}s ago (threshold ${ScidIdleSeconds}s)."
    } elseif ($sierraRunning) {
        $result.note = "Sierra process present but newest SCID idle for ${ageSec}s (best-effort closed/idle)."
    } elseif ($result.scidAppearsIdle) {
        $result.note = "Sierra not running; newest SCID idle for ${ageSec}s."
    } else {
        $result.note = "Sierra not running but newest SCID still fresh (${ageSec}s) - unexpected growth or clock skew."
    }
    return [pscustomobject]$result
}

function Write-CatchUpCommands {
    param([string]$ContractHint)
    Write-Log "--- Operator-gated catch-up commands (NOT executed unless -ConfirmCatchUp) ---"
    Write-Log "1) Ensure the-desk-mcp is STOPPED before any destructive DB maintenance;"
    Write-Log "   catch-up ingest/backfill via MCP requires the MCP server (Cursor session)."
    Write-Log "2) MCP tools (from a Cursor agent with The Desk MCP connected):"
    Write-Log "     ingest_raw_ticks_from_scid  { onlyGaps: true }"
    Write-Log "     get_raw_tick_ingest_gaps"
    Write-Log "     backfill_history            { /* session/event window as needed */ }"
    Write-Log "     get_research_summary"
    Write-Log "3) CLI status (read-only):"
    Write-Log "     `$env:USERPROFILE = '$UserProfilePath'"
    Write-Log "     & '$script:StorageExe' --status"
    if ($ContractHint) {
        Write-Log "   Contract hint from config: $ContractHint"
    }
    Write-Log "--- End catch-up command list ---"
}

try {
    if ([string]::IsNullOrWhiteSpace($UserProfilePath)) {
        Write-Error "UserProfilePath is empty."
        exit $script:EXIT_CONFIG
    }

    Initialize-Logging
    Write-Log "Friday data readiness starting. ConfirmCatchUp=$ConfirmCatchUp DryRun=$DryRun"

    if (-not (Test-Path "X:\") -and -not $DryRun) {
        Write-Log "STORAGE FAILURE: X: not mounted; cannot write readiness manifest to X:\TheDesk\logs."
        exit $script:EXIT_STORAGE
    }

    $contractHint = Get-TomlHint -Key "active_symbol_override"
    if (-not $contractHint) { $contractHint = Get-TomlHint -Key "symbol" }
    $baseSymbol = Get-TomlHint -Key "base_symbol"

    $mcpRunning = [bool](Get-Process -Name $script:McpProcessName -ErrorAction SilentlyContinue)
    Write-Log "MCP running: $mcpRunning"

    $sierra = Test-SierraRecordingIdle
    Write-Log ("Sierra check: {0}" -f ($sierra | ConvertTo-Json -Compress))

    $tFree = Get-DriveFreeGb -Letter "T"
    $xFree = Get-DriveFreeGb -Letter "X"
    Write-Log "Free space T:=$tFree GB X:=$xFree GB"

    $backupLatest = $null
    $backupDir = "X:\TheDesk\backups"
    if (Test-Path -LiteralPath $backupDir) {
        $backupLatest = Get-ChildItem -LiteralPath $backupDir -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
        if ($backupLatest) {
            Write-Log "Latest backup: $($backupLatest.FullName) @ $($backupLatest.LastWriteTime.ToString('o'))"
        }
    }

    $statusOk = $null
    if (Test-Path -LiteralPath $script:StorageExe) {
        if ($DryRun) {
            Write-Log "DRYRUN: would run the-desk-storage --status"
            $statusOk = $true
        } else {
            $old = $env:USERPROFILE
            try {
                $env:USERPROFILE = $UserProfilePath
                $out = & $script:StorageExe --status 2>&1
                foreach ($line in $out) { Write-Log $line.ToString() }
                $statusOk = ($LASTEXITCODE -eq 0)
                if (-not $statusOk) {
                    Write-Log "storage --status exited $LASTEXITCODE"
                }
            } finally {
                $env:USERPROFILE = $old
            }
        }
    } else {
        Write-Log "WARNING: storage binary missing at $script:StorageExe"
    }

    Write-CatchUpCommands -ContractHint $contractHint

    $catchUpRefusedMcp = $false
    $catchUpNote = "Ingest/backfill not executed by this script without explicit operator MCP/CLI action."
    if ($ConfirmCatchUp) {
        if ($mcpRunning) {
            $catchUpRefusedMcp = $true
            $catchUpNote = "ConfirmCatchUp refused: the-desk-mcp is running (exit 2). Catch-up remains operator-approved only."
            Write-Log "DEFERRED (exit 2): $catchUpNote"
        } elseif ($DryRun) {
            Write-Log "DRYRUN: would acknowledge operator-approved catch-up (still no mutation in DryRun)."
        } else {
            Write-Log "NOTE: -ConfirmCatchUp acknowledges operator approval, but this script does not auto-run ingest/backfill."
            Write-Log "Run the printed MCP tools from an agent session (MCP must be up for those tools)."
            Write-Log "Refusing silent mutation: catch-up remains a manual MCP/CLI step after this gate."
        }
    }

    $manifestDir = if (Test-Path "X:\") {
        "X:\TheDesk\logs"
    } else {
        Join-Path $UserProfilePath ".the-desk\logs"
    }
    New-Item -ItemType Directory -Force -Path $manifestDir | Out-Null
    $stamp = Get-Date -Format "yyyyMMdd"
    $manifestPath = Join-Path $manifestDir ("weekend-readiness-{0}.json" -f $stamp)

    $manifest = [ordered]@{
        timestamp        = (Get-Date).ToString("o")
        host             = $env:COMPUTERNAME
        dryRun           = [bool]$DryRun
        confirmCatchUp   = [bool]$ConfirmCatchUp
        catchUpRefusedMcp = $catchUpRefusedMcp
        contractHint     = $contractHint
        baseSymbol       = $baseSymbol
        mcpRunning       = $mcpRunning
        sierra           = $sierra
        freeSpaceGb      = [ordered]@{ T = $tFree; X = $xFree }
        backupLatestPath = if ($backupLatest) { $backupLatest.FullName } else { $null }
        backupLatestTime = if ($backupLatest) { $backupLatest.LastWriteTime.ToString("o") } else { $null }
        storageStatusOk  = $statusOk
        logPath          = $script:LogPath
        catchUpGated     = $true
        note             = $catchUpNote
    }
    ($manifest | ConvertTo-Json -Depth 8) | Set-Content -LiteralPath $manifestPath -Encoding UTF8
    Write-Log "Wrote readiness manifest: $manifestPath"

    if ($catchUpRefusedMcp) {
        exit $script:EXIT_DEFERRED
    }
    if ($null -ne $tFree -and $tFree -lt 40) {
        Write-Log "STORAGE FAILURE: T: free space $tFree GB below 40 GB critical."
        exit $script:EXIT_STORAGE
    }
    if ((Test-Path "X:\") -and $null -ne $xFree -and $xFree -lt 50) {
        Write-Log "STORAGE FAILURE: X: free space $xFree GB below 50 GB critical."
        exit $script:EXIT_STORAGE
    }

    exit $script:EXIT_SUCCESS
} catch {
    Write-Log "GENERAL FAILURE: $($_.Exception.Message)"
    exit $script:EXIT_GENERAL
}
