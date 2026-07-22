<#
.SYNOPSIS
  Saturday storage maintenance: archive warm raw_ticks + prune depth_events.

.NOTES
  Exit codes (see Desk-ExitCodes.ps1):
    0 success
    2 deferred (MCP writer active / maintenance blocked)
    3 config error
    4 integrity failure
    5 storage failure (e.g. X: missing / temp unusable)
    1 general failure

  Triggers use machine local wall-clock. This workstation is expected to be Central Time.
#>
param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$UserProfilePath = $env:USERPROFILE,
    [int]$WarmRetentionDays = 30,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "Desk-ExitCodes.ps1")

$script:StorageExe = Join-Path $RepoRoot "target_alt\release\the-desk-storage.exe"
$script:LogPath = $null
$script:LogDir = $null
$script:ArchiveTempDir = "X:\TheDesk\temp"
$script:McpProcessName = "the-desk-mcp"

function Initialize-Logging {
    if (Test-Path "X:\") {
        $script:LogDir = "X:\TheDesk\logs"
    } elseif ($DryRun) {
        $script:LogDir = Join-Path $UserProfilePath ".the-desk\logs"
    } else {
        $script:LogDir = Join-Path $env:TEMP "TheDesk\logs"
    }
    New-Item -ItemType Directory -Force -Path $script:LogDir | Out-Null
    $script:LogPath = Join-Path $script:LogDir ("weekly-archive-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) { Initialize-Logging }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $script:LogPath -Value $line
    Write-Host $line
}

function Get-MarkerDir {
    if (Test-Path "X:\") {
        $dir = "X:\TheDesk\logs"
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        return $dir
    }
    if ($DryRun) {
        $dir = Join-Path $UserProfilePath ".the-desk\logs"
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        return $dir
    }
    return $null
}

function Write-MaintenanceMarker {
    param(
        [Parameter(Mandatory)][ValidateSet("success", "deferred")][string]$Kind,
        [hashtable]$Extra = @{}
    )
    $dir = Get-MarkerDir
    if (-not $dir) {
        Write-Log "WARNING: cannot write $Kind marker (no X: and not DryRun)."
        return
    }
    $name = if ($Kind -eq "success") { "last-successful-maintenance.json" } else { "last-deferred-maintenance.json" }
    $path = Join-Path $dir $name
    $payload = [ordered]@{
        kind        = $Kind
        timestamp   = (Get-Date).ToString("o")
        host        = $env:COMPUTERNAME
        dryRun      = [bool]$DryRun
        script      = $PSCommandPath
        logPath     = $script:LogPath
        userProfile = $UserProfilePath
        cutoffDays  = $WarmRetentionDays
    }
    foreach ($key in $Extra.Keys) {
        $payload[$key] = $Extra[$key]
    }
    ($payload | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath $path -Encoding UTF8
    Write-Log "Wrote $Kind marker: $path"
}

function Get-ArchiveCutoff {
    $et = [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, "Eastern Standard Time")
    return $et.Date.AddDays(-1 * $WarmRetentionDays).ToString("yyyy-MM-dd")
}

function Test-StorageSupportsAbortIfMcp {
    if (-not (Test-Path -LiteralPath $script:StorageExe)) {
        return $false
    }
    try {
        $help = & $script:StorageExe --help 2>&1 | Out-String
        return $help -match "--abort-if-mcp"
    } catch {
        return $false
    }
}

function Ensure-ArchiveTemp {
    if (Test-Path "X:\") {
        New-Item -ItemType Directory -Force -Path $script:ArchiveTempDir | Out-Null
        New-Item -ItemType Directory -Force -Path "X:\TheDesk\logs" | Out-Null
        return $script:ArchiveTempDir
    }

    $msg = "Archive drive X: is missing; cannot create X:\TheDesk\temp for SQLite temp files during maintenance."
    if ($DryRun) {
        $fallback = Join-Path $UserProfilePath ".the-desk\temp"
        New-Item -ItemType Directory -Force -Path $fallback | Out-Null
        Write-Log "DRYRUN: $msg Falling back to $fallback for dry-run only."
        return $fallback
    }

    Write-Log "STORAGE FAILURE: $msg"
    exit $script:EXIT_STORAGE
}

function Invoke-Storage {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    if ($DryRun) {
        Write-Log "DRYRUN: would run $script:StorageExe $($Arguments -join ' ')"
        return
    }
    if (-not (Test-Path -LiteralPath $script:StorageExe)) {
        throw "Missing storage binary: $script:StorageExe"
    }

    $oldUserProfile = $env:USERPROFILE
    $oldTmp = $env:TMP
    $oldTemp = $env:TEMP
    $oldSqliteTmp = $env:SQLITE_TMPDIR
    try {
        $env:USERPROFILE = $UserProfilePath
        $tempDir = Ensure-ArchiveTemp
        $env:TMP = $tempDir
        $env:TEMP = $tempDir
        $env:SQLITE_TMPDIR = $tempDir
        Write-Log "Running: $script:StorageExe $($Arguments -join ' ')"
        $output = & $script:StorageExe @Arguments 2>&1
        foreach ($line in $output) {
            Write-Log $line.ToString()
        }
        if ($LASTEXITCODE -ne 0) {
            throw "the-desk-storage failed with exit code $LASTEXITCODE."
        }
    } finally {
        $env:USERPROFILE = $oldUserProfile
        $env:TMP = $oldTmp
        $env:TEMP = $oldTemp
        $env:SQLITE_TMPDIR = $oldSqliteTmp
    }
}

try {
    if ([string]::IsNullOrWhiteSpace($UserProfilePath)) {
        Write-Error "UserProfilePath is empty; set -UserProfilePath or USERPROFILE."
        exit $script:EXIT_CONFIG
    }

    Initialize-Logging
    Write-Log "Starting weekly archive. UserProfilePath=$UserProfilePath DryRun=$DryRun"

    if (Get-Process -Name $script:McpProcessName -ErrorAction SilentlyContinue) {
        $reason = "the-desk-mcp is running; weekly archive deferred rather than fighting the live writer."
        Write-Log "DEFERRED (exit 2): $reason"
        Write-MaintenanceMarker -Kind deferred -Extra @{
            reason   = "mcp_writer_active"
            exitCode = $script:EXIT_DEFERRED
            message  = $reason
        }
        exit $script:EXIT_DEFERRED
    }

    # Fail fast on missing X: before invoking the binary (non-dry-run).
    $null = Ensure-ArchiveTemp

    $cutoff = Get-ArchiveCutoff
    $maintainArgs = @("--maintain", "--cutoff", $cutoff)
    if (Test-StorageSupportsAbortIfMcp) {
        $maintainArgs += "--abort-if-mcp"
        Write-Log "Storage binary supports --abort-if-mcp; will pass it on maintain."
    } else {
        Write-Log "Storage binary does not advertise --abort-if-mcp; relying on pre-check only."
    }

    Invoke-Storage "--status" "--cutoff" $cutoff
    Invoke-Storage @maintainArgs
    Write-Log "Weekly archive completed."
    Write-MaintenanceMarker -Kind success -Extra @{
        cutoff          = $cutoff
        warmRetentionDays = $WarmRetentionDays
        abortIfMcpPassed  = ($maintainArgs -contains "--abort-if-mcp")
    }
    exit $script:EXIT_SUCCESS
} catch {
    Write-Log "GENERAL FAILURE: $($_.Exception.Message)"
    exit $script:EXIT_GENERAL
}
