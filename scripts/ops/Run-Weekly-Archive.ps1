param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$UserProfilePath = "C:\Users\alecw",
    [int]$WarmRetentionDays = 30,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$script:StorageExe = Join-Path $RepoRoot "target_alt\release\the-desk-storage.exe"
$script:LogPath = $null

function Initialize-Logging {
    $logDir = if (Test-Path "X:\") { "X:\TheDesk\logs" } else { Join-Path $env:TEMP "TheDesk\logs" }
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $script:LogPath = Join-Path $logDir ("weekly-archive-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) { Initialize-Logging }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    $line | Tee-Object -FilePath $script:LogPath -Append
}

function Get-ArchiveCutoff {
    $et = [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, "Eastern Standard Time")
    return $et.Date.AddDays(-1 * $WarmRetentionDays).ToString("yyyy-MM-dd")
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
        New-Item -ItemType Directory -Force -Path "X:\TheDesk\temp" | Out-Null
        $env:TMP = "X:\TheDesk\temp"
        $env:TEMP = "X:\TheDesk\temp"
        $env:SQLITE_TMPDIR = "X:\TheDesk\temp"
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

Initialize-Logging
Write-Log "Starting weekly archive."

if (Get-Process -Name "the-desk-mcp" -ErrorAction SilentlyContinue) {
    Write-Log "the-desk-mcp is running; weekly archive aborting rather than fighting the live writer."
    exit 0
}

$cutoff = Get-ArchiveCutoff
Invoke-Storage "--status" "--cutoff" $cutoff
Invoke-Storage "--maintain" "--cutoff" $cutoff
Write-Log "Weekly archive completed."
