param(
    [Parameter(Mandatory)]
    [ValidateSet("Watchdog", "Close", "Open")]
    [string]$Action,
    [switch]$DryRun,
    [string]$SierraExe = "T:\SierraChart\SierraChart_64.exe",
    [string]$Chartbook = "T:\SierraChart\Data\LightweightChartBook2026.Cht"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$script:LogPath = $null
$script:ProcessName = "SierraChart_64"

function Initialize-Logging {
    $logDir = if (Test-Path "X:\") { "X:\TheDesk\logs" } else { Join-Path $env:TEMP "TheDesk\logs" }
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $script:LogPath = Join-Path $logDir ("sierra-lifecycle-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) { Initialize-Logging }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $script:LogPath -Value $line
    Write-Host $line
}

function Get-EasternNow {
    return [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, "Eastern Standard Time")
}

function Test-TradingWeekWindow {
    param([Parameter(Mandatory)][datetime]$EasternNow)
    $time = $EasternNow.TimeOfDay
    switch ($EasternNow.DayOfWeek) {
        "Sunday" { return $time -ge ([TimeSpan]::FromHours(18)) }
        "Monday" { return $true }
        "Tuesday" { return $true }
        "Wednesday" { return $true }
        "Thursday" { return $true }
        "Friday" { return $time -lt ([TimeSpan]::FromHours(17)) }
        default { return $false }
    }
}

function Open-Sierra {
    $existing = Get-Process -Name $script:ProcessName -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Log "Sierra Chart is already running."
        return
    }
    if (-not (Test-Path -LiteralPath $SierraExe)) {
        throw "Sierra executable not found: $SierraExe"
    }
    if ($DryRun) {
        Write-Log "DRYRUN: would launch $SierraExe"
        return
    }
    Start-Process -FilePath $SierraExe -WorkingDirectory (Split-Path -Parent $SierraExe)
    Write-Log "Launched Sierra Chart. Expected chartbook on startup: $Chartbook"
}

function Close-Sierra {
    $processes = Get-Process -Name $script:ProcessName -ErrorAction SilentlyContinue
    if (-not $processes) {
        Write-Log "Sierra Chart is not running."
        return
    }
    if ($DryRun) {
        Write-Log "DRYRUN: would close Sierra Chart gracefully, then force-kill after 60 seconds if needed."
        return
    }

    foreach ($process in $processes) {
        Write-Log "Sending CloseMainWindow to Sierra PID $($process.Id)."
        [void]$process.CloseMainWindow()
    }

    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Process -Name $script:ProcessName -ErrorAction SilentlyContinue) -and (Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 2
    }

    $remaining = Get-Process -Name $script:ProcessName -ErrorAction SilentlyContinue
    if ($remaining) {
        Write-Log "Sierra did not exit within 60 seconds; force-killing remaining process(es)."
        $remaining | Stop-Process -Force
    } else {
        Write-Log "Sierra Chart exited cleanly."
    }
}

Initialize-Logging
$et = Get-EasternNow
Write-Log "Action=$Action ET=$($et.ToString('yyyy-MM-dd HH:mm:ss'))"

switch ($Action) {
    "Watchdog" {
        if (Test-TradingWeekWindow -EasternNow $et) {
            Write-Log "Inside trading-week window; ensuring Sierra is running."
            Open-Sierra
        } else {
            Write-Log "Outside trading-week window; watchdog is intentionally idle."
        }
    }
    "Open" { Open-Sierra }
    "Close" { Close-Sierra }
}
