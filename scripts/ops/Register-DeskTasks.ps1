<#
.SYNOPSIS
  Register or verify The Desk Windows Scheduled Tasks under \TheDesk\.

.NOTES
  Triggers use **machine local wall-clock** time. This trading workstation is expected
  to be set to Central Time. Descriptions document ET equivalents.

  Exit codes (see Desk-ExitCodes.ps1):
    0 success
    3 config error / verify mismatches
    1 general failure

  Use -DryRun to preview registration without calling Register-ScheduledTask.
  Use -Verify to check existing tasks (existence, enabled, action, cwd, principal, triggers).
#>
param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$UserProfilePath = $env:USERPROFILE,
    [switch]$EnableMonthlyCompaction,
    [switch]$Verify,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "Desk-ExitCodes.ps1")

$taskPath = "\TheDesk\"
$powershell = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$currentUser = "$env:USERDOMAIN\$env:USERNAME"

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Quote-Path {
    param([Parameter(Mandatory)][string]$Path)
    return '"' + $Path.Replace('"', '\"') + '"'
}

function New-PowerShellAction {
    param(
        [Parameter(Mandatory)][string]$ScriptPath,
        [string]$Arguments = ""
    )
    $arg = "-NoProfile -ExecutionPolicy Bypass -File $(Quote-Path $ScriptPath)"
    if ($Arguments) {
        $arg = "$arg $Arguments"
    }
    return New-ScheduledTaskAction -Execute $powershell -Argument $arg -WorkingDirectory $RepoRoot
}

function Register-DeskTask {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)]$Action,
        [Parameter(Mandatory)]$Triggers,
        [Parameter(Mandatory)]$Principal,
        [Parameter(Mandatory)]$Settings,
        [Parameter(Mandatory)][string]$Description,
        [switch]$Disabled
    )

    if ($DryRun) {
        Write-Host "DRYRUN: would register $taskPath$Name"
        Write-Host "        $Description"
        Write-Host "        Action: $($Action.Execute) $($Action.Arguments)"
        Write-Host "        WorkingDirectory: $($Action.WorkingDirectory)"
        return
    }

    $task = New-ScheduledTask -Action $Action -Trigger $Triggers -Principal $Principal -Settings $Settings -Description $Description
    Register-ScheduledTask -TaskName $Name -TaskPath $taskPath -InputObject $task -Force | Out-Null
    if ($Disabled) {
        Disable-ScheduledTask -TaskName $Name -TaskPath $taskPath | Out-Null
    }
    Write-Host "Registered $taskPath$Name"
}

function Get-ExpectedTaskSpecs {
    $sierraScript = Join-Path $RepoRoot "scripts\ops\Sierra-Lifecycle.ps1"
    $weeklyScript = Join-Path $RepoRoot "scripts\ops\Run-Weekly-Archive.ps1"
    $diskScript = Join-Path $RepoRoot "scripts\ops\Check-Disk-Space.ps1"
    $reclaimScript = Join-Path $RepoRoot "scripts\ops\Reclaim-Storage.ps1"
    $fridayScript = Join-Path $RepoRoot "scripts\ops\Invoke-FridayDataReadiness.ps1"
    $healthScript = Join-Path $RepoRoot "scripts\ops\Invoke-DeskHealthCheck.ps1"

    $weeklyArgs = "-UserProfilePath $(Quote-Path $UserProfilePath)"
    $fridayArgs = "-UserProfilePath $(Quote-Path $UserProfilePath)"
    $healthDailyArgs = "-Mode Daily -UserProfilePath $(Quote-Path $UserProfilePath)"
    $healthSundayArgs = "-Mode SundayPreOpen -UserProfilePath $(Quote-Path $UserProfilePath)"
    $diskArgs = "-DriveLetter T -ThresholdGb 40 -AlsoCheckX -XThresholdGb 50"
    $monthlyArgs = "-Mode CompactOnly -Confirm -AbortIfMcpRunning -MinFreelistGb 50"

    return @(
        [pscustomobject]@{
            Name = "Sierra Watchdog"
            ScriptPath = $sierraScript
            Arguments = "-Action Watchdog"
            PrincipalKind = "Interactive"
            ExpectEnabled = $true
            Description = "Interactive-session watchdog: launches Sierra during Sun 18:00 ET through Fri 17:00 ET if it is not running. Idle during daily 17:00-18:00 ET (16:00-17:00 CT) halt. Triggers are local wall-clock."
        },
        [pscustomobject]@{
            Name = "Engine Watchdog"
            ScriptPath = $engineScript
            Arguments = "-Action Watchdog"
            PrincipalKind = "Interactive"
            ExpectEnabled = $true
            Description = "SIL-M2a: keep the-desk-engine running for Globex overnight coverage when [sil].engine_mode=external. MCP disconnect must not stop ingest."
        },
        [pscustomobject]@{
            Name = "Sierra Weekend Close"
            ScriptPath = $sierraScript
            Arguments = "-Action Close"
            PrincipalKind = "Interactive"
            ExpectEnabled = $true
            Description = "Friday 16:10 local (Central on this box) / 17:10 ET graceful Sierra close."
        },
        [pscustomobject]@{
            Name = "Sierra Sunday Open"
            ScriptPath = $sierraScript
            Arguments = "-Action Open"
            PrincipalKind = "Interactive"
            ExpectEnabled = $true
            Description = "Sunday 16:50 local (Central) / 17:50 ET Sierra pre-open launch."
        },
        [pscustomobject]@{
            Name = "Friday Data Readiness"
            ScriptPath = $fridayScript
            Arguments = $fridayArgs
            PrincipalKind = "System"
            ExpectEnabled = $true
            Description = "Friday 16:20 local (Central) / 17:20 ET - post-Sierra-close readiness check + weekend-readiness manifest. Non-destructive; catch-up is operator-gated."
        },
        [pscustomobject]@{
            Name = "Weekly Storage Archive"
            ScriptPath = $weeklyScript
            Arguments = $weeklyArgs
            PrincipalKind = "System"
            ExpectEnabled = $true
            Description = "Saturday storage maintenance (task name retained). Saturdays from 09:00 local Central (10:00 ET), retrying hourly for 10h: archive old raw_ticks + depth prune. Exit 2 if MCP writer is up (deferred, not silent success)."
        },
        [pscustomobject]@{
            Name = "Sunday Pre-Open Readiness"
            ScriptPath = $healthScript
            Arguments = $healthSundayArgs
            PrincipalKind = "System"
            ExpectEnabled = $true
            Description = "Sunday 16:40 local (Central) / 17:40 ET - health check Mode=SundayPreOpen before Globex. Fails if weekend maintenance marker is missing/stale."
        },
        [pscustomobject]@{
            Name = "Storage Health Check"
            ScriptPath = $healthScript
            Arguments = $healthDailyArgs
            PrincipalKind = "System"
            ExpectEnabled = $true
            Description = "Every 6 hours (SYSTEM): Invoke-DeskHealthCheck -Mode Daily. Local wall-clock cadence."
        },
        [pscustomobject]@{
            Name = "T Drive Low Disk Alarm"
            ScriptPath = $diskScript
            Arguments = $diskArgs
            PrincipalKind = "System"
            ExpectEnabled = $true
            Description = "Every 30 minutes: alert if T: below 40 GB or X: below 50 GB free."
        },
        [pscustomobject]@{
            Name = "Monthly Storage Compaction"
            ScriptPath = $reclaimScript
            Arguments = $monthlyArgs
            PrincipalKind = "System"
            ExpectEnabled = [bool]$EnableMonthlyCompaction
            Description = "Optional disabled-by-default monthly VACUUM INTO + swap when freelist exceeds 50 GB. Saturday 11:00 local Central / 12:00 ET."
        }
    )
}

function Invoke-VerifyMode {
    $specs = Get-ExpectedTaskSpecs
    $pass = 0
    $fail = 0
    $rows = @()

    foreach ($spec in $specs) {
        $issues = New-Object System.Collections.Generic.List[string]
        try {
            $task = Get-ScheduledTask -TaskPath $taskPath -TaskName $spec.Name -ErrorAction Stop
            $info = Get-ScheduledTaskInfo -TaskPath $taskPath -TaskName $spec.Name -ErrorAction SilentlyContinue

            if ($spec.ExpectEnabled -and $task.State -eq "Disabled") {
                $issues.Add("expected Enabled but State=Disabled")
            }
            if (-not $spec.ExpectEnabled -and $task.State -ne "Disabled") {
                $issues.Add("expected Disabled but State=$($task.State)")
            }

            $action = $task.Actions | Select-Object -First 1
            if (-not $action) {
                $issues.Add("no action defined")
            } else {
                if ($action.Execute -ne $powershell) {
                    $issues.Add("Execute mismatch: $($action.Execute)")
                }
                $expectedArg = "-NoProfile -ExecutionPolicy Bypass -File $(Quote-Path $spec.ScriptPath)"
                if ($spec.Arguments) { $expectedArg = "$expectedArg $($spec.Arguments)" }
                if ($action.Arguments -ne $expectedArg) {
                    $issues.Add("Arguments mismatch.`n    expected: $expectedArg`n    actual:   $($action.Arguments)")
                }
                if ($action.WorkingDirectory -ne $RepoRoot) {
                    $issues.Add("WorkingDirectory mismatch: $($action.WorkingDirectory) (expected $RepoRoot)")
                }
                if (-not (Test-Path -LiteralPath $spec.ScriptPath)) {
                    $issues.Add("script path missing on disk: $($spec.ScriptPath)")
                }
            }

            $principal = $task.Principal
            if ($spec.PrincipalKind -eq "System") {
                if ($principal.UserId -notmatch "^(SYSTEM|NT AUTHORITY\\SYSTEM)$") {
                    $issues.Add("Principal UserId=$($principal.UserId) (expected SYSTEM)")
                }
            } else {
                if ([string]::IsNullOrWhiteSpace($principal.UserId)) {
                    $issues.Add("Interactive principal UserId empty")
                }
            }

            if (-not $task.Triggers -or @($task.Triggers).Count -lt 1) {
                $issues.Add("no triggers defined")
            }

            $nextRun = if ($info) { $info.NextRunTime } else { $null }
            if ($spec.ExpectEnabled -and $nextRun -and $nextRun -lt (Get-Date).AddYears(-1)) {
                $issues.Add("NextRunTime looks unset/invalid: $nextRun")
            }

            if ($issues.Count -eq 0) {
                $pass++
                $status = "PASS"
                $detail = "State=$($task.State); NextRun=$nextRun"
            } else {
                $fail++
                $status = "FAIL"
                $detail = ($issues -join "; ")
            }
        } catch {
            $fail++
            $status = "FAIL"
            $detail = "Task missing under $taskPath : $($_.Exception.Message)"
        }

        $rows += [pscustomobject]@{ Task = $spec.Name; Result = $status; Detail = $detail }
        Write-Host ("{0}: {1} - {2}" -f $status, $spec.Name, $detail)
    }

    Write-Host ""
    Write-Host "Verify summary: PASS=$pass FAIL=$fail"
    $rows | Format-Table -AutoSize | Out-Host
    if ($fail -gt 0) {
        exit $script:EXIT_CONFIG
    }
    exit $script:EXIT_SUCCESS
}

if ($Verify) {
    Invoke-VerifyMode
}

if (-not $DryRun -and -not (Test-IsAdministrator)) {
    throw "Register-DeskTasks.ps1 must run from an elevated PowerShell session (or pass -DryRun / -Verify)."
}

if ([string]::IsNullOrWhiteSpace($UserProfilePath)) {
    Write-Error "UserProfilePath is empty."
    exit $script:EXIT_CONFIG
}

$sierraScript = Join-Path $RepoRoot "scripts\ops\Sierra-Lifecycle.ps1"
$engineScript = Join-Path $RepoRoot "scripts\ops\Engine-Lifecycle.ps1"
$weeklyScript = Join-Path $RepoRoot "scripts\ops\Run-Weekly-Archive.ps1"
$diskScript = Join-Path $RepoRoot "scripts\ops\Check-Disk-Space.ps1"
$reclaimScript = Join-Path $RepoRoot "scripts\ops\Reclaim-Storage.ps1"
$fridayScript = Join-Path $RepoRoot "scripts\ops\Invoke-FridayDataReadiness.ps1"
$healthScript = Join-Path $RepoRoot "scripts\ops\Invoke-DeskHealthCheck.ps1"

$sierraPrincipal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited
$systemPrincipal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

$sierraSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
$maintenanceSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 12)
$alarmSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 5)
$healthSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 15)

# NOTE: All -At times below are machine **local wall-clock**. This workstation is expected
# to use Central Time. ET equivalents are documented in each task Description.
$watchdogLogon = New-ScheduledTaskTrigger -AtLogOn -User $currentUser
$watchdogRepeat = New-ScheduledTaskTrigger -Once -At ((Get-Date).Date.AddMinutes(1)) -RepetitionInterval (New-TimeSpan -Minutes 4) -RepetitionDuration (New-TimeSpan -Days 3650)
$fridayClose = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At "16:10"
$fridayReadiness = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At "16:20"
$sundayOpen = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At "16:50"
$sundayPreOpen = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At "16:40"
$weeklyArchive = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At "09:00"
# Option A (retry-until-idle): repeat hourly across Saturday so maintenance runs the
# first hour the MCP writer is down. Run-Weekly-Archive.ps1 exits 2 (deferred) if the MCP
# is up; the archive + rowid depth prune are idempotent, so later hourly fires that find
# nothing aged-out are cheap no-ops. No forced app shutdown.
$weeklyArchive.Repetition = (New-ScheduledTaskTrigger -Once -At "09:00" `
        -RepetitionInterval (New-TimeSpan -Hours 1) `
        -RepetitionDuration (New-TimeSpan -Hours 10)).Repetition
$diskAlarm = New-ScheduledTaskTrigger -Once -At ((Get-Date).Date.AddMinutes(2)) -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Days 3650)
$healthDaily = New-ScheduledTaskTrigger -Once -At ((Get-Date).Date.AddMinutes(3)) -RepetitionInterval (New-TimeSpan -Hours 6) -RepetitionDuration (New-TimeSpan -Days 3650)
$monthlyCompact = New-ScheduledTaskTrigger -Weekly -WeeksInterval 4 -DaysOfWeek Saturday -At "11:00"

$weeklyArgs = "-UserProfilePath $(Quote-Path $UserProfilePath)"
$fridayArgs = "-UserProfilePath $(Quote-Path $UserProfilePath)"
$healthDailyArgs = "-Mode Daily -UserProfilePath $(Quote-Path $UserProfilePath)"
$healthSundayArgs = "-Mode SundayPreOpen -UserProfilePath $(Quote-Path $UserProfilePath)"
$diskArgs = "-DriveLetter T -ThresholdGb 40 -AlsoCheckX -XThresholdGb 50"

Register-DeskTask `
    -Name "Sierra Watchdog" `
    -Action (New-PowerShellAction -ScriptPath $sierraScript -Arguments "-Action Watchdog") `
    -Triggers @($watchdogLogon, $watchdogRepeat) `
    -Principal $sierraPrincipal `
    -Settings $sierraSettings `
    -Description "Interactive-session watchdog: launches Sierra during Sun 18:00 ET through Fri 17:00 ET if it is not running. Idle during daily 17:00-18:00 ET (16:00-17:00 CT) halt. Triggers are local wall-clock (Central on this box)."

Register-DeskTask `
    -Name "Engine Watchdog" `
    -Action (New-PowerShellAction -ScriptPath $engineScript -Arguments "-Action Watchdog") `
    -Triggers @($watchdogLogon, $watchdogRepeat) `
    -Principal $sierraPrincipal `
    -Settings $sierraSettings `
    -Description "SIL-M2a: keep the-desk-engine running whenever Sierra records (including Globex overnight). Use with [sil].engine_mode=external; embedded MCP mode remains the rollback."

Register-DeskTask `
    -Name "Sierra Weekend Close" `
    -Action (New-PowerShellAction -ScriptPath $sierraScript -Arguments "-Action Close") `
    -Triggers $fridayClose `
    -Principal $sierraPrincipal `
    -Settings $sierraSettings `
    -Description "Friday 16:10 local Central / 17:10 ET graceful Sierra close."

Register-DeskTask `
    -Name "Sierra Sunday Open" `
    -Action (New-PowerShellAction -ScriptPath $sierraScript -Arguments "-Action Open") `
    -Triggers $sundayOpen `
    -Principal $sierraPrincipal `
    -Settings $sierraSettings `
    -Description "Sunday 16:50 local Central / 17:50 ET Sierra pre-open launch."

Register-DeskTask `
    -Name "Friday Data Readiness" `
    -Action (New-PowerShellAction -ScriptPath $fridayScript -Arguments $fridayArgs) `
    -Triggers $fridayReadiness `
    -Principal $systemPrincipal `
    -Settings $healthSettings `
    -Description "Friday 16:20 local Central / 17:20 ET post-close readiness. Writes weekend-readiness-YYYYMMDD.json. Catch-up ingest/backfill remains operator-gated."

Register-DeskTask `
    -Name "Weekly Storage Archive" `
    -Action (New-PowerShellAction -ScriptPath $weeklyScript -Arguments $weeklyArgs) `
    -Triggers $weeklyArchive `
    -Principal $systemPrincipal `
    -Settings $maintenanceSettings `
    -Description "Saturday storage maintenance (name retained: Weekly Storage Archive). 09:00 local Central / 10:00 ET, hourly retry 10h. Exit 2 if MCP up (deferred marker); success writes last-successful-maintenance.json."

Register-DeskTask `
    -Name "Sunday Pre-Open Readiness" `
    -Action (New-PowerShellAction -ScriptPath $healthScript -Arguments $healthSundayArgs) `
    -Triggers $sundayPreOpen `
    -Principal $systemPrincipal `
    -Settings $healthSettings `
    -Description "Sunday 16:40 local Central / 17:40 ET health check (Mode=SundayPreOpen) before Globex open."

Register-DeskTask `
    -Name "Storage Health Check" `
    -Action (New-PowerShellAction -ScriptPath $healthScript -Arguments $healthDailyArgs) `
    -Triggers $healthDaily `
    -Principal $systemPrincipal `
    -Settings $healthSettings `
    -Description "Every 6 hours SYSTEM: Invoke-DeskHealthCheck -Mode Daily (disk, tasks, maintenance marker, backups)."

Register-DeskTask `
    -Name "T Drive Low Disk Alarm" `
    -Action (New-PowerShellAction -ScriptPath $diskScript -Arguments $diskArgs) `
    -Triggers $diskAlarm `
    -Principal $systemPrincipal `
    -Settings $alarmSettings `
    -Description "Every 30 minutes: logs/alerts if T: below 40 GB or X: below 50 GB free. SYSTEM msg targets interactive user when possible."

$monthlyArgs = "-Mode CompactOnly -Confirm -AbortIfMcpRunning -MinFreelistGb 50"
Register-DeskTask `
    -Name "Monthly Storage Compaction" `
    -Action (New-PowerShellAction -ScriptPath $reclaimScript -Arguments $monthlyArgs) `
    -Triggers $monthlyCompact `
    -Principal $systemPrincipal `
    -Settings $maintenanceSettings `
    -Description "Optional disabled-by-default monthly VACUUM INTO + swap when freelist exceeds 50 GB. Saturday 11:00 local Central / 12:00 ET." `
    -Disabled:(!$EnableMonthlyCompaction)

# Post-registration / dry-run summary table
$monthlyEnabledLabel = if ($EnableMonthlyCompaction) { "yes" } else { "no-default" }
$summary = @(
    [pscustomobject]@{ Task = "Sierra Watchdog";              When = "Logon + every 4m";           Account = "Interactive"; Enabled = "yes" }
    [pscustomobject]@{ Task = "Sierra Weekend Close";         When = "Fri 16:10 local (CT)";       Account = "Interactive"; Enabled = "yes" }
    [pscustomobject]@{ Task = "Friday Data Readiness";        When = "Fri 16:20 local (CT)";       Account = "SYSTEM";      Enabled = "yes" }
    [pscustomobject]@{ Task = "Weekly Storage Archive";       When = "Sat 09:00 + hourly 10h CT";  Account = "SYSTEM";      Enabled = "yes" }
    [pscustomobject]@{ Task = "Sunday Pre-Open Readiness";    When = "Sun 16:40 local (CT)";       Account = "SYSTEM";      Enabled = "yes" }
    [pscustomobject]@{ Task = "Sierra Sunday Open";           When = "Sun 16:50 local (CT)";       Account = "Interactive"; Enabled = "yes" }
    [pscustomobject]@{ Task = "Storage Health Check";         When = "Every 6 hours";              Account = "SYSTEM";      Enabled = "yes" }
    [pscustomobject]@{ Task = "T Drive Low Disk Alarm";       When = "Every 30 minutes";           Account = "SYSTEM";      Enabled = "yes" }
    [pscustomobject]@{ Task = "Monthly Storage Compaction";   When = "Sat 11:00 CT / 4-week";      Account = "SYSTEM";      Enabled = $monthlyEnabledLabel }
)

Write-Host ""
Write-Host "Desk task plan (local wall-clock = Central on this workstation; ET = +1h):"
$summary | Format-Table -AutoSize | Out-Host

if ($DryRun) {
    Write-Host "DRYRUN complete. No scheduled tasks were changed."
    Write-Host "UserProfilePath that would be passed: $UserProfilePath"
} else {
    Write-Host "Desk tasks registered under $taskPath"
    Write-Host "UserProfilePath passed to maintenance/readiness scripts: $UserProfilePath"
    if (-not $EnableMonthlyCompaction) {
        Write-Host "Monthly Storage Compaction was registered disabled."
    }
}
