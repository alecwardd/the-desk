<#
.SYNOPSIS
  Best-effort, non-mutating health checks for The Desk storage/ops posture.

.PARAMETER Mode
  Daily            - routine every-6h checks
  FridayReadiness  - pre-weekend posture
  SundayPreOpen    - confirm Saturday maintenance landed before Globex
  Verify           - stricter task + marker verification

.NOTES
  Exit codes (see Desk-ExitCodes.ps1):
    0 success (no critical failures)
    2 deferred-related finding treated as critical in SundayPreOpen when maintenance is stale
    3 config error
    4 integrity failure (missing tasks / stale critical markers)
    5 storage failure (drive missing / free space below critical)
    1 general failure

  Thresholds (critical unless noted):
    T: free space          < 40 GB
    X: free space          < 50 GB  (warning < 100 GB)
    Last successful maint  Daily: warn > 8d, critical > 14d
                           SundayPreOpen / FridayReadiness: critical > 8d
                           Verify: critical if missing or > 8d
    Latest backup age      warn > 48h, critical > 7d (if backups dir exists)
    Scheduled tasks        missing under \TheDesk\ = critical for Verify/Sunday/Friday;
                           Daily reports missing as critical too
    MCP process            informational (expected during week; note-only)
    the-desk-storage --status  optional; failure is warning unless Mode=Verify (then critical)

  Logs: X:\TheDesk\logs\health-*.log (TEMP fallback if X: missing)
#>
param(
    [ValidateSet("Daily", "FridayReadiness", "SundayPreOpen", "Verify")]
    [string]$Mode = "Daily",
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$UserProfilePath = $env:USERPROFILE,
    [double]$TDriveCriticalGb = 40,
    [double]$XDriveCriticalGb = 50,
    [double]$XDriveWarnGb = 100,
    [int]$MaintWarnDays = 8,
    [int]$MaintCriticalDays = 14,
    [int]$BackupWarnHours = 48,
    [int]$BackupCriticalDays = 7,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "Desk-ExitCodes.ps1")

$script:LogPath = $null
$script:StorageExe = Join-Path $RepoRoot "target_alt\release\the-desk-storage.exe"
$script:TaskPath = "\TheDesk\"
$script:ExpectedTasks = @(
    "Sierra Watchdog",
    "Sierra Weekend Close",
    "Sierra Sunday Open",
    "Weekly Storage Archive",
    "T Drive Low Disk Alarm",
    "Friday Data Readiness",
    "Sunday Pre-Open Readiness",
    "Storage Health Check"
)

$script:Findings = New-Object System.Collections.Generic.List[object]
$script:CriticalCount = 0
$script:WarningCount = 0

function Initialize-Logging {
    $logDir = if (Test-Path "X:\") { "X:\TheDesk\logs" } else { Join-Path $env:TEMP "TheDesk\logs" }
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $script:LogPath = Join-Path $logDir ("health-{0}-{1}.log" -f $Mode, (Get-Date -Format "yyyyMMdd-HHmmss"))
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) { Initialize-Logging }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $script:LogPath -Value $line
    Write-Host $line
}

function Add-Finding {
    param(
        [Parameter(Mandatory)][ValidateSet("PASS", "WARN", "FAIL")][string]$Severity,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Detail
    )
    $script:Findings.Add([pscustomobject]@{ Severity = $Severity; Name = $Name; Detail = $Detail })
    Write-Log ("{0}: {1} - {2}" -f $Severity, $Name, $Detail)
    if ($Severity -eq "FAIL") { $script:CriticalCount++ }
    elseif ($Severity -eq "WARN") { $script:WarningCount++ }
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

function Test-DriveSpace {
    param(
        [Parameter(Mandatory)][string]$Letter,
        [Parameter(Mandatory)][double]$CriticalGb,
        [double]$WarnGb = 0
    )
    if (-not (Test-Path "${Letter}:\")) {
        Add-Finding -Severity "FAIL" -Name "${Letter}: presence" -Detail "Drive ${Letter}: is not mounted."
        return
    }
    $free = Get-DriveFreeGb -Letter $Letter
    if ($null -eq $free) {
        Add-Finding -Severity "FAIL" -Name "${Letter}: free space" -Detail "Could not read free space for ${Letter}:."
        return
    }
    if ($free -lt $CriticalGb) {
        Add-Finding -Severity "FAIL" -Name "${Letter}: free space" -Detail "${Letter}: has $free GB free (critical < $CriticalGb GB)."
    } elseif ($WarnGb -gt 0 -and $free -lt $WarnGb) {
        Add-Finding -Severity "WARN" -Name "${Letter}: free space" -Detail "${Letter}: has $free GB free (warn < $WarnGb GB)."
    } else {
        Add-Finding -Severity "PASS" -Name "${Letter}: free space" -Detail "${Letter}: has $free GB free."
    }
}

function Test-ScheduledTasks {
    $strict = $Mode -in @("Verify", "FridayReadiness", "SundayPreOpen", "Daily")
    foreach ($name in $script:ExpectedTasks) {
        try {
            $task = Get-ScheduledTask -TaskPath $script:TaskPath -TaskName $name -ErrorAction Stop
            $state = $task.State.ToString()
            if ($name -eq "Monthly Storage Compaction") {
                Add-Finding -Severity "PASS" -Name "task:$name" -Detail "Present (state=$state)."
                continue
            }
            if ($state -eq "Disabled" -and $strict) {
                Add-Finding -Severity "WARN" -Name "task:$name" -Detail "Present but Disabled."
            } else {
                Add-Finding -Severity "PASS" -Name "task:$name" -Detail "Present (state=$state)."
            }
        } catch {
            $sev = if ($strict) { "FAIL" } else { "WARN" }
            Add-Finding -Severity $sev -Name "task:$name" -Detail "Missing under $script:TaskPath."
        }
    }
}

function Test-McpPresence {
    $proc = Get-Process -Name "the-desk-mcp" -ErrorAction SilentlyContinue
    if ($proc) {
        Add-Finding -Severity "PASS" -Name "mcp process" -Detail "the-desk-mcp is running (pid=$(($proc | Select-Object -First 1).Id))."
    } else {
        Add-Finding -Severity "PASS" -Name "mcp process" -Detail "the-desk-mcp is not running (informational)."
    }
}

function Get-MarkerPath {
    param([Parameter(Mandatory)][string]$FileName)
    $candidates = @()
    if (Test-Path "X:\") { $candidates += (Join-Path "X:\TheDesk\logs" $FileName) }
    $candidates += (Join-Path $UserProfilePath ".the-desk\logs\$FileName")
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) { return $c }
    }
    return $null
}

function Test-MaintenanceMarkerAge {
    $path = Get-MarkerPath -FileName "last-successful-maintenance.json"
    $criticalDays = if ($Mode -in @("SundayPreOpen", "FridayReadiness", "Verify")) { $MaintWarnDays } else { $MaintCriticalDays }
    $warnDays = $MaintWarnDays

    if (-not $path) {
        $sev = if ($Mode -in @("SundayPreOpen", "Verify", "FridayReadiness")) { "FAIL" } else { "WARN" }
        Add-Finding -Severity $sev -Name "maintenance marker" -Detail "last-successful-maintenance.json not found."
        return
    }

    try {
        $json = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
        $ts = [datetime]::Parse($json.timestamp)
        $age = (Get-Date) - $ts
        $ageDays = [math]::Round($age.TotalDays, 2)
        if ($age.TotalDays -gt $criticalDays) {
            Add-Finding -Severity "FAIL" -Name "maintenance marker" -Detail "Last success $ageDays days ago at $path (critical > $criticalDays d)."
        } elseif ($age.TotalDays -gt $warnDays) {
            Add-Finding -Severity "WARN" -Name "maintenance marker" -Detail "Last success $ageDays days ago at $path (warn > $warnDays d)."
        } else {
            Add-Finding -Severity "PASS" -Name "maintenance marker" -Detail "Last success $ageDays days ago ($path)."
        }
    } catch {
        Add-Finding -Severity "FAIL" -Name "maintenance marker" -Detail "Unreadable marker at $path : $($_.Exception.Message)"
    }

    $deferred = Get-MarkerPath -FileName "last-deferred-maintenance.json"
    if ($deferred) {
        try {
            $djson = Get-Content -LiteralPath $deferred -Raw | ConvertFrom-Json
            $dts = [datetime]::Parse($djson.timestamp)
            if ($path) {
                $sjson = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
                $sts = [datetime]::Parse($sjson.timestamp)
                if ($dts -gt $sts -and $Mode -eq "SundayPreOpen") {
                    Add-Finding -Severity "FAIL" -Name "maintenance deferred" -Detail "Deferred marker newer than success ($deferred). Weekend maintenance may have skipped."
                } elseif ($dts -gt $sts) {
                    Add-Finding -Severity "WARN" -Name "maintenance deferred" -Detail "Deferred marker newer than success ($deferred)."
                }
            }
        } catch {
            Add-Finding -Severity "WARN" -Name "maintenance deferred" -Detail "Could not parse deferred marker: $($_.Exception.Message)"
        }
    }
}

function Test-BackupHeader {
    param([System.IO.FileInfo]$File)
    # A zeroed page 0 passes every age check; only the magic bytes prove a restore point.
    try {
        $stream = [System.IO.File]::Open($File.FullName, 'Open', 'Read', 'ReadWrite')
        try {
            $magic = New-Object byte[] 16
            $read = $stream.Read($magic, 0, 16)
            if ($read -lt 16) { return $false }
            return ([System.Text.Encoding]::ASCII.GetString($magic, 0, 15) -eq 'SQLite format 3') -and ($magic[15] -eq 0)
        } finally {
            $stream.Dispose()
        }
    } catch {
        return $false
    }
}

function Test-BackupAge {
    $backupDir = "X:\TheDesk\backups"
    if (-not (Test-Path -LiteralPath $backupDir)) {
        Add-Finding -Severity "WARN" -Name "backup dir" -Detail "X:\TheDesk\backups not present."
        return
    }
    $backups = @(Get-ChildItem -LiteralPath $backupDir -File -Filter 'desk-*.db' -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending)
    if (-not $backups) {
        Add-Finding -Severity "WARN" -Name "backup age" -Detail "No desk-*.db backup files under $backupDir."
        return
    }
    $corrupt = @($backups | Where-Object { -not (Test-BackupHeader $_) })
    foreach ($bad in $corrupt) {
        Add-Finding -Severity "FAIL" -Name "backup header" -Detail "$($bad.Name) is missing the SQLite header magic (page 0 zeroed or truncated) — not a restore point."
    }
    $latestValid = $backups | Where-Object { $corrupt -notcontains $_ } | Select-Object -First 1
    if (-not $latestValid) {
        Add-Finding -Severity "FAIL" -Name "backup age" -Detail "NO header-valid backups under $backupDir — create a verified backup before any destructive maintenance."
        return
    }
    $age = (Get-Date) - $latestValid.LastWriteTime
    $ageHours = [math]::Round($age.TotalHours, 1)
    if ($age.TotalDays -gt $BackupCriticalDays) {
        Add-Finding -Severity "FAIL" -Name "backup age" -Detail "Latest header-valid backup $($latestValid.Name) is $ageHours h old (critical > $BackupCriticalDays d)."
    } elseif ($age.TotalHours -gt $BackupWarnHours) {
        Add-Finding -Severity "WARN" -Name "backup age" -Detail "Latest header-valid backup $($latestValid.Name) is $ageHours h old (warn > $BackupWarnHours h)."
    } else {
        Add-Finding -Severity "PASS" -Name "backup age" -Detail "Latest header-valid backup $($latestValid.Name) is $ageHours h old."
    }
}

function Test-StorageStatus {
    if (-not (Test-Path -LiteralPath $script:StorageExe)) {
        Add-Finding -Severity "WARN" -Name "storage binary" -Detail "Missing $script:StorageExe"
        return
    }
    if ($DryRun) {
        Add-Finding -Severity "PASS" -Name "storage --status" -Detail "DRYRUN: would run the-desk-storage --status"
        return
    }
    try {
        $old = $env:USERPROFILE
        $env:USERPROFILE = $UserProfilePath
        $output = & $script:StorageExe --status 2>&1
        $env:USERPROFILE = $old
        if ($LASTEXITCODE -ne 0) {
            $sev = if ($Mode -eq "Verify") { "FAIL" } else { "WARN" }
            Add-Finding -Severity $sev -Name "storage --status" -Detail "Exit $LASTEXITCODE. $output"
        } else {
            Add-Finding -Severity "PASS" -Name "storage --status" -Detail "OK."
            Write-Log ("storage --status output: {0}" -f (($output | ForEach-Object { $_.ToString() }) -join " | "))
        }
    } catch {
        $sev = if ($Mode -eq "Verify") { "FAIL" } else { "WARN" }
        Add-Finding -Severity $sev -Name "storage --status" -Detail $_.Exception.Message
    }
}

try {
    Initialize-Logging
    Write-Log "Health check Mode=$Mode DryRun=$DryRun"

    Test-DriveSpace -Letter "T" -CriticalGb $TDriveCriticalGb
    Test-DriveSpace -Letter "X" -CriticalGb $XDriveCriticalGb -WarnGb $XDriveWarnGb
    Test-ScheduledTasks
    Test-McpPresence
    Test-MaintenanceMarkerAge
    Test-BackupAge
    Test-StorageStatus

    Write-Log "Summary: critical=$script:CriticalCount warnings=$script:WarningCount"
    $script:Findings | Format-Table -AutoSize | Out-String | ForEach-Object { Write-Log $_.TrimEnd() }

    if ($script:CriticalCount -gt 0) {
        # Prefer storage exit when drive/space failed; else integrity.
        $hasStorage = $script:Findings | Where-Object { $_.Severity -eq "FAIL" -and ($_.Name -match "presence|free space") }
        if ($hasStorage) {
            exit $script:EXIT_STORAGE
        }
        exit $script:EXIT_INTEGRITY
    }
    exit $script:EXIT_SUCCESS
} catch {
    Write-Log "GENERAL FAILURE: $($_.Exception.Message)"
    exit $script:EXIT_GENERAL
}
