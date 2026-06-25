param(
    [string]$DriveLetter = "T",
    [double]$ThresholdGb = 40,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$script:LogPath = $null

function Initialize-Logging {
    $logDir = if (Test-Path "X:\") { "X:\TheDesk\logs" } else { Join-Path $env:TEMP "TheDesk\logs" }
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $script:LogPath = Join-Path $logDir ("disk-space-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) { Initialize-Logging }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    $line | Tee-Object -FilePath $script:LogPath -Append
}

function Send-DiskAlert {
    param([Parameter(Mandatory)][string]$Message)
    Write-Log "ALERT: $Message"
    if ($DryRun) {
        Write-Log "DRYRUN: would write Windows event log and local user message."
        return
    }

    try {
        if (-not [System.Diagnostics.EventLog]::SourceExists("TheDeskOps")) {
            New-EventLog -LogName Application -Source "TheDeskOps"
        }
        Write-EventLog -LogName Application -Source "TheDeskOps" -EventId 4001 -EntryType Warning -Message $Message
    } catch {
        Write-Log "Could not write Windows event log: $($_.Exception.Message)"
    }

    try {
        $user = $env:USERNAME
        if ($user) {
            & msg.exe $user $Message 2>$null
        }
    } catch {
        Write-Log "Could not send local user message: $($_.Exception.Message)"
    }
}

Initialize-Logging
$volume = Get-Volume -DriveLetter $DriveLetter -ErrorAction Stop
$freeGb = [math]::Round($volume.SizeRemaining / 1GB, 2)
$sizeGb = [math]::Round($volume.Size / 1GB, 2)
Write-Log "$DriveLetter`: free=$freeGb GB size=$sizeGb GB threshold=$ThresholdGb GB"

if ($freeGb -lt $ThresholdGb) {
    Send-DiskAlert "The Desk warning: drive $DriveLetter`: has $freeGb GB free, below $ThresholdGb GB. Sierra recording can halt if the drive fills."
}
