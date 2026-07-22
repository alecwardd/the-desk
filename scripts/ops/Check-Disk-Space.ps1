<#
.SYNOPSIS
  Low-disk alarm for T: (and optionally X:).

.NOTES
  Exit codes (see Desk-ExitCodes.ps1):
    0 success (check completed; low disk is alerted but still exits 0 unless -FailOnLow)
    5 storage failure when -FailOnLow and below threshold
    1 general failure

  When running as SYSTEM, msg.exe targets the interactive console user via CIM when possible;
  otherwise falls back to the Application event log only.
#>
param(
    [string]$DriveLetter = "T",
    [double]$ThresholdGb = 40,
    [switch]$AlsoCheckX,
    [double]$XThresholdGb = 50,
    [switch]$FailOnLow,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "Desk-ExitCodes.ps1")

$script:LogPath = $null
$script:LowDisk = $false

function Initialize-Logging {
    $logDir = if (Test-Path "X:\") { "X:\TheDesk\logs" } else { Join-Path $env:TEMP "TheDesk\logs" }
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $script:LogPath = Join-Path $logDir ("disk-space-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) { Initialize-Logging }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $script:LogPath -Value $line
    Write-Host $line
}

function Get-InteractiveSessionUser {
    # Prefer the logged-on console user (SYSTEM tasks see USERNAME=SYSTEM).
    try {
        $cs = Get-CimInstance -ClassName Win32_ComputerSystem -ErrorAction Stop
        if ($cs.UserName) {
            # DOMAIN\user → user for msg.exe
            $parts = $cs.UserName -split "\\"
            return $parts[-1]
        }
    } catch {
        Write-Log "CIM Win32_ComputerSystem.UserName unavailable: $($_.Exception.Message)"
    }

    try {
        $explorer = Get-CimInstance -ClassName Win32_Process -Filter "Name = 'explorer.exe'" -ErrorAction Stop |
            Select-Object -First 1
        if ($explorer) {
            $owner = Invoke-CimMethod -InputObject $explorer -MethodName GetOwner -ErrorAction Stop
            if ($owner -and $owner.User) {
                return $owner.User
            }
        }
    } catch {
        Write-Log "CIM explorer owner lookup failed: $($_.Exception.Message)"
    }

    if ($env:USERNAME -and $env:USERNAME -notmatch "^(SYSTEM|LOCAL SERVICE|NETWORK SERVICE)$") {
        return $env:USERNAME
    }
    return $null
}

function Send-DiskAlert {
    param([Parameter(Mandatory)][string]$Message)
    Write-Log "ALERT: $Message"
    $script:LowDisk = $true
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
        $user = Get-InteractiveSessionUser
        if ($user) {
            Write-Log "Sending msg.exe to interactive user '$user'."
            & msg.exe $user $Message 2>$null
        } else {
            Write-Log "No interactive user for msg.exe; event log is the alert fallback."
        }
    } catch {
        Write-Log "Could not send local user message: $($_.Exception.Message)"
    }
}

function Test-DriveThreshold {
    param(
        [Parameter(Mandatory)][string]$Letter,
        [Parameter(Mandatory)][double]$Threshold
    )
    if (-not (Test-Path "${Letter}:\")) {
        Write-Log "Drive ${Letter}: not present; skipping."
        return
    }
    $volume = Get-Volume -DriveLetter $Letter -ErrorAction Stop
    $freeGb = [math]::Round($volume.SizeRemaining / 1GB, 2)
    $sizeGb = [math]::Round($volume.Size / 1GB, 2)
    Write-Log "${Letter}: free=$freeGb GB size=$sizeGb GB threshold=$Threshold GB"

    if ($freeGb -lt $Threshold) {
        Send-DiskAlert "The Desk warning: drive ${Letter}: has $freeGb GB free, below $Threshold GB. Sierra recording or archive maintenance can halt if the drive fills."
    }
}

try {
    Initialize-Logging
    Test-DriveThreshold -Letter $DriveLetter -Threshold $ThresholdGb
    if ($AlsoCheckX -or $DriveLetter -eq "X") {
        if ($DriveLetter -ne "X") {
            Test-DriveThreshold -Letter "X" -Threshold $XThresholdGb
        }
    }

    if ($FailOnLow -and $script:LowDisk) {
        exit $script:EXIT_STORAGE
    }
    exit $script:EXIT_SUCCESS
} catch {
    Write-Log "GENERAL FAILURE: $($_.Exception.Message)"
    exit $script:EXIT_GENERAL
}
