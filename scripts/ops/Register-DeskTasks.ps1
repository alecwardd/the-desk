param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [switch]$EnableMonthlyCompaction,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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
        return
    }

    $task = New-ScheduledTask -Action $Action -Trigger $Triggers -Principal $Principal -Settings $Settings -Description $Description
    Register-ScheduledTask -TaskName $Name -TaskPath $taskPath -InputObject $task -Force | Out-Null
    if ($Disabled) {
        Disable-ScheduledTask -TaskName $Name -TaskPath $taskPath | Out-Null
    }
    Write-Host "Registered $taskPath$Name"
}

if (-not $DryRun -and -not (Test-IsAdministrator)) {
    throw "Register-DeskTasks.ps1 must run from an elevated PowerShell session."
}

$sierraScript = Join-Path $RepoRoot "scripts\ops\Sierra-Lifecycle.ps1"
$weeklyScript = Join-Path $RepoRoot "scripts\ops\Run-Weekly-Archive.ps1"
$diskScript = Join-Path $RepoRoot "scripts\ops\Check-Disk-Space.ps1"
$reclaimScript = Join-Path $RepoRoot "scripts\ops\Reclaim-Storage.ps1"

$sierraPrincipal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited
$systemPrincipal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

$sierraSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
$maintenanceSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 12)
$alarmSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

$watchdogLogon = New-ScheduledTaskTrigger -AtLogOn -User $currentUser
$watchdogRepeat = New-ScheduledTaskTrigger -Once -At ((Get-Date).Date.AddMinutes(1)) -RepetitionInterval (New-TimeSpan -Minutes 4) -RepetitionDuration (New-TimeSpan -Days 3650)
$fridayClose = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At "16:10"
$sundayOpen = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At "16:50"
$weeklyArchive = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At "09:00"
$diskAlarm = New-ScheduledTaskTrigger -Once -At ((Get-Date).Date.AddMinutes(2)) -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Days 3650)
$monthlyCompact = New-ScheduledTaskTrigger -Weekly -WeeksInterval 4 -DaysOfWeek Saturday -At "11:00"

Register-DeskTask `
    -Name "Sierra Watchdog" `
    -Action (New-PowerShellAction -ScriptPath $sierraScript -Arguments "-Action Watchdog") `
    -Triggers @($watchdogLogon, $watchdogRepeat) `
    -Principal $sierraPrincipal `
    -Settings $sierraSettings `
    -Description "Interactive-session watchdog: launches Sierra during Sun 18:00 ET through Fri 17:00 ET if it is not running."

Register-DeskTask `
    -Name "Sierra Weekend Close" `
    -Action (New-PowerShellAction -ScriptPath $sierraScript -Arguments "-Action Close") `
    -Triggers $fridayClose `
    -Principal $sierraPrincipal `
    -Settings $sierraSettings `
    -Description "Friday 17:10 ET / 16:10 Central graceful Sierra close."

Register-DeskTask `
    -Name "Sierra Sunday Open" `
    -Action (New-PowerShellAction -ScriptPath $sierraScript -Arguments "-Action Open") `
    -Triggers $sundayOpen `
    -Principal $sierraPrincipal `
    -Settings $sierraSettings `
    -Description "Sunday 17:50 ET / 16:50 Central Sierra pre-open launch."

Register-DeskTask `
    -Name "Weekly Storage Archive" `
    -Action (New-PowerShellAction -ScriptPath $weeklyScript) `
    -Triggers $weeklyArchive `
    -Principal $systemPrincipal `
    -Settings $maintenanceSettings `
    -Description "Saturday 10:00 ET / 09:00 Central archive/delete of old raw_ticks. Aborts if the MCP writer is running."

Register-DeskTask `
    -Name "T Drive Low Disk Alarm" `
    -Action (New-PowerShellAction -ScriptPath $diskScript -Arguments "-DriveLetter T -ThresholdGb 40") `
    -Triggers $diskAlarm `
    -Principal $systemPrincipal `
    -Settings $alarmSettings `
    -Description "Logs and alerts every 30 minutes if T: free space drops below 40 GB."

$monthlyArgs = "-Mode CompactOnly -Confirm -AbortIfMcpRunning -MinFreelistGb 50"
Register-DeskTask `
    -Name "Monthly Storage Compaction" `
    -Action (New-PowerShellAction -ScriptPath $reclaimScript -Arguments $monthlyArgs) `
    -Triggers $monthlyCompact `
    -Principal $systemPrincipal `
    -Settings $maintenanceSettings `
    -Description "Optional disabled-by-default monthly VACUUM INTO + swap when freelist exceeds 50 GB." `
    -Disabled:(!$EnableMonthlyCompaction)

if ($DryRun) {
    Write-Host "DRYRUN complete. No scheduled tasks were changed."
} else {
    Write-Host "Desk tasks registered under $taskPath"
    if (-not $EnableMonthlyCompaction) {
        Write-Host "Monthly Storage Compaction was registered disabled."
    }
}
