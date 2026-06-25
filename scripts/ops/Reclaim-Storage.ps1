param(
    [ValidateSet("FullReclaim", "PrepareArchiveDrive", "CompactOnly", "VerifyOnly")]
    [string]$Mode = "FullReclaim",
    [switch]$Confirm,
    [switch]$DryRun,
    [switch]$WhatIf,
    [switch]$AbortIfMcpRunning,
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$UserProfilePath = "C:\Users\alecw",
    [int]$WarmRetentionDays = 30,
    [double]$MinFreelistGb = 0
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$script:ArchiveDrive = "X"
$script:ArchiveRoot = "X:\TheDesk"
$script:ArchiveDir = "X:\TheDesk\archive"
$script:ArchiveStateDir = "X:\TheDesk\state"
$script:ArchiveTempDir = "X:\TheDesk\temp"
$script:ArchiveLogDir = "X:\TheDesk\logs"
$script:OldArchiveDir = "T:\TheDesk\archive"
$script:DataDb = Join-Path $UserProfilePath ".the-desk\data.db"
$script:ConfigPath = Join-Path $UserProfilePath ".the-desk\config.toml"
$script:StorageExe = Join-Path $RepoRoot "target_alt\release\the-desk-storage.exe"
$script:McpProcessName = "the-desk-mcp"
$script:LogPath = $null

function Initialize-Logging {
    $logDir = if (Test-Path "X:\") { $script:ArchiveLogDir } else { Join-Path $env:TEMP "TheDesk\logs" }
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $script:LogPath = Join-Path $logDir "reclaim-storage-$stamp.log"
}

function Write-Log {
    param([Parameter(Mandatory)][string]$Message)
    if (-not $script:LogPath) {
        Initialize-Logging
    }
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $script:LogPath -Value $line
    Write-Host $line
}

function Copy-TempLogToArchive {
    if (-not (Test-Path "X:\")) {
        return
    }
    New-Item -ItemType Directory -Force -Path $script:ArchiveLogDir | Out-Null
    if ($script:LogPath -and -not $script:LogPath.StartsWith("X:\", [System.StringComparison]::OrdinalIgnoreCase)) {
        $dest = Join-Path $script:ArchiveLogDir (Split-Path -Leaf $script:LogPath)
        Copy-Item -LiteralPath $script:LogPath -Destination $dest -Force
        $script:LogPath = $dest
        Write-Log "Moved logging to $dest"
    }
}

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Require-Admin {
    if (-not (Test-IsAdministrator)) {
        throw "Reclaim-Storage.ps1 must run from an elevated PowerShell session for $Mode."
    }
}

function Invoke-Step {
    param(
        [Parameter(Mandatory)][string]$Message,
        [Parameter(Mandatory)][scriptblock]$Action,
        [switch]$Destructive
    )

    Write-Log $Message
    if ($DryRun -or $WhatIf) {
        Write-Log "DRYRUN: skipped step."
        return
    }
    if ($Destructive -and -not $Confirm) {
        throw "Refusing destructive step without explicit -Confirm: $Message"
    }
    & $Action
}

function Get-EasternNow {
    return [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, "Eastern Standard Time")
}

function Get-ArchiveCutoff {
    return (Get-EasternNow).Date.AddDays(-1 * $WarmRetentionDays).ToString("yyyy-MM-dd")
}

function Assert-StorageExe {
    if (-not (Test-Path -LiteralPath $script:StorageExe)) {
        if ($DryRun -or $WhatIf) {
            Write-Log "DRYRUN warning: storage binary is not built at $script:StorageExe"
            return
        }
        throw "Missing storage binary: $script:StorageExe. Build with CARGO_TARGET_DIR=target_alt cargo build --release --bin the-desk-storage."
    }
}

function Invoke-StorageCommand {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    $display = "$script:StorageExe $($Arguments -join ' ')"
    if ($DryRun -or $WhatIf) {
        Write-Log "DRYRUN: would run $display"
        return @()
    }

    Assert-StorageExe
    $oldUserProfile = $env:USERPROFILE
    $oldTmp = $env:TMP
    $oldTemp = $env:TEMP
    $oldSqliteTmp = $env:SQLITE_TMPDIR
    try {
        $env:USERPROFILE = $UserProfilePath
        New-Item -ItemType Directory -Force -Path $script:ArchiveTempDir | Out-Null
        $env:TMP = $script:ArchiveTempDir
        $env:TEMP = $script:ArchiveTempDir
        $env:SQLITE_TMPDIR = $script:ArchiveTempDir
        Write-Log "Running: $display"
        $output = & $script:StorageExe @Arguments 2>&1
        foreach ($line in $output) {
            Write-Log $line.ToString()
        }
        if ($LASTEXITCODE -ne 0) {
            throw "Storage command failed with exit code ${LASTEXITCODE}: $display"
        }
        return $output
    } finally {
        $env:USERPROFILE = $oldUserProfile
        $env:TMP = $oldTmp
        $env:TEMP = $oldTemp
        $env:SQLITE_TMPDIR = $oldSqliteTmp
    }
}

function Assert-ArchiveDiskSafety {
    $disk = Get-Disk -Number 2 -ErrorAction Stop
    $sizeTb = $disk.Size / 1TB
    Write-Log ("Disk 2: FriendlyName='{0}', BusType={1}, Size={2:N2} TiB, PartitionStyle={3}, IsBoot={4}, IsSystem={5}" -f $disk.FriendlyName, $disk.BusType, $sizeTb, $disk.PartitionStyle, $disk.IsBoot, $disk.IsSystem)

    if ($disk.FriendlyName -notlike "Seagate*") {
        throw "Disk 2 friendly name does not match Seagate*: $($disk.FriendlyName)"
    }
    if ($disk.BusType -ne "USB") {
        throw "Disk 2 is not USB: $($disk.BusType)"
    }
    if ($disk.Size -lt 1.6TB -or $disk.Size -gt 2.1TB) {
        throw "Disk 2 size is outside the expected ~1.8 TB range: $($disk.Size) bytes"
    }
    if ($disk.IsBoot -or $disk.IsSystem) {
        throw "Disk 2 is marked boot/system; refusing to format."
    }

    $badLetters = @()
    $partitions = Get-Partition -DiskNumber 2 -ErrorAction SilentlyContinue
    foreach ($partition in $partitions) {
        if ($partition.DriveLetter -in @("C", "T")) {
            $badLetters += $partition.DriveLetter
        }
    }
    if ($badLetters.Count -gt 0) {
        throw "Disk 2 has protected drive letter(s) $($badLetters -join ', '); refusing to format."
    }
}

function Assert-ArchiveVolume {
    $driveRoot = "$($script:ArchiveDrive):\"
    if (-not (Test-Path -LiteralPath $driveRoot)) {
        throw "$driveRoot is not mounted."
    }
    $volume = Get-Volume -DriveLetter $script:ArchiveDrive -ErrorAction Stop
    if ($volume.FileSystem -ne "NTFS") {
        throw "X: is not NTFS; found $($volume.FileSystem)."
    }
    if ($volume.FileSystemLabel -ne "DeskArchive") {
        throw "X: label is not DeskArchive; found '$($volume.FileSystemLabel)'."
    }
    $freeTb = $volume.SizeRemaining / 1TB
    Write-Log ("Verified X: DeskArchive NTFS, free={0:N2} TiB." -f $freeTb)
}

function Test-ArchiveVolumeReady {
    try {
        Assert-ArchiveVolume
        return $true
    } catch {
        Write-Log "Archive volume is not ready yet: $($_.Exception.Message)"
        return $false
    }
}

function Prepare-ArchiveDrive {
    if (Test-ArchiveVolumeReady) {
        if ($DryRun -or $WhatIf) {
            Write-Log "DRYRUN: would ensure archive directories exist on X:."
        } else {
            New-Item -ItemType Directory -Force -Path $script:ArchiveDir, $script:ArchiveStateDir, $script:ArchiveTempDir, $script:ArchiveLogDir | Out-Null
            Copy-TempLogToArchive
        }
        return
    }

    Assert-ArchiveDiskSafety
    Invoke-Step "Formatting Disk 2 as X: DeskArchive." {
        Set-Disk -Number 2 -IsOffline $false -ErrorAction SilentlyContinue
        Set-Disk -Number 2 -IsReadOnly $false -ErrorAction SilentlyContinue
        Clear-Disk -Number 2 -RemoveData -RemoveOEM -Confirm:$false
        Initialize-Disk -Number 2 -PartitionStyle GPT
        New-Partition -DiskNumber 2 -UseMaximumSize -DriveLetter $script:ArchiveDrive | Out-Null
        Format-Volume -DriveLetter $script:ArchiveDrive -FileSystem NTFS -NewFileSystemLabel "DeskArchive" -Confirm:$false -Force | Out-Null
    } -Destructive

    if ($DryRun -or $WhatIf) {
        Write-Log "DRYRUN: archive drive was not mounted; skipping post-format X: verification and directory creation."
        return
    }

    Assert-ArchiveVolume
    New-Item -ItemType Directory -Force -Path $script:ArchiveDir, $script:ArchiveStateDir, $script:ArchiveTempDir, $script:ArchiveLogDir | Out-Null
    Copy-TempLogToArchive
}

function Assert-McpStopped {
    $processes = Get-Process -Name $script:McpProcessName -ErrorAction SilentlyContinue
    if ($processes) {
        throw "$script:McpProcessName is still running; refusing DB maintenance."
    }
}

function Stop-Mcp {
    if ($AbortIfMcpRunning -and (Get-Process -Name $script:McpProcessName -ErrorAction SilentlyContinue)) {
        throw "$script:McpProcessName is running and -AbortIfMcpRunning was set."
    }

    Invoke-Step "Stopping any $script:McpProcessName process. Sierra Chart is left running." {
        $processes = Get-Process -Name $script:McpProcessName -ErrorAction SilentlyContinue
        if (-not $processes) {
            Write-Log "$script:McpProcessName is not running."
            return
        }
        $processes | Stop-Process -Force
        $deadline = (Get-Date).AddSeconds(30)
        while ((Get-Process -Name $script:McpProcessName -ErrorAction SilentlyContinue) -and (Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 1
        }
        Assert-McpStopped
    }
}

function Assert-DbUnlocked {
    if ($DryRun -or $WhatIf) {
        Write-Log "DRYRUN: would open $script:DataDb exclusively to confirm it is unlocked."
        return
    }
    $stream = [System.IO.File]::Open($script:DataDb, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
    $stream.Close()
    Write-Log "Confirmed database is unlocked: $script:DataDb"
}

function Update-ColdArchiveConfig {
    Invoke-Step "Updating cold_archive_dir in $script:ConfigPath." {
        $content = [System.IO.File]::ReadAllText($script:ConfigPath)
        $replacement = 'cold_archive_dir = "X:\\TheDesk\\archive"'
        $newContent = [regex]::Replace($content, '(?m)^cold_archive_dir\s*=\s*".*"\s*$', $replacement, 1)
        if ($newContent -eq $content -and $content -notmatch '(?m)^cold_archive_dir\s*=') {
            throw "cold_archive_dir was not found in $script:ConfigPath"
        }
        [System.IO.File]::WriteAllText($script:ConfigPath, $newContent, [System.Text.UTF8Encoding]::new($false))
    }
}

function Move-ExistingArchives {
    Invoke-Step "Moving existing cold archive files from $script:OldArchiveDir to $script:ArchiveDir." {
        New-Item -ItemType Directory -Force -Path $script:ArchiveDir | Out-Null
        if (-not (Test-Path -LiteralPath $script:OldArchiveDir)) {
            Write-Log "No old archive directory found at $script:OldArchiveDir."
            return
        }
        $files = Get-ChildItem -LiteralPath $script:OldArchiveDir -File -ErrorAction Stop
        foreach ($file in $files) {
            $dest = Join-Path $script:ArchiveDir $file.Name
            if (Test-Path -LiteralPath $dest) {
                throw "Destination archive already exists: $dest"
            }
        }
        foreach ($file in $files) {
            Move-Item -LiteralPath $file.FullName -Destination $script:ArchiveDir
            Write-Log "Moved archive file $($file.Name) to $script:ArchiveDir."
        }
    }
}

function Get-TDriveFreeBytes {
    return (Get-PSDrive -Name T).Free
}

function Get-FreelistGb {
    $output = Invoke-StorageCommand @("--status")
    foreach ($line in $output) {
        $text = $line.ToString()
        if ($text -match 'freelist_size=([0-9.]+)\s+GB') {
            return [double]$Matches[1]
        }
    }
    throw "Could not parse freelist_size from the-desk-storage --status output."
}

function Remove-DbSidecars {
    foreach ($path in @("$($script:DataDb)-wal", "$($script:DataDb)-shm")) {
        if (Test-Path -LiteralPath $path) {
            Remove-Item -LiteralPath $path -Force
            Write-Log "Removed old SQLite sidecar $path."
        }
    }
}

function Invoke-ArchiveOldTicks {
    param([Parameter(Mandatory)][string]$Cutoff)
    Invoke-StorageCommand @("--status", "--cutoff", $Cutoff) | Out-Null
    Invoke-StorageCommand @("--maintain", "--cutoff", $Cutoff) | Out-Null
}

function Invoke-CompactIntoArchive {
    param([Parameter(Mandatory)][string]$Cutoff)
    $compacted = $script:ArchiveStateDir + "\data_compacted.db"
    if ((Test-Path -LiteralPath $compacted) -and -not ($DryRun -or $WhatIf)) {
        throw "Compacted destination already exists: $compacted"
    }
    Invoke-StorageCommand @("--compact-into", $compacted, "--cutoff", $Cutoff) | Out-Null
    return $compacted
}

function Verify-DatabaseCopy {
    param(
        [Parameter(Mandatory)][string]$CopyPath,
        [Parameter(Mandatory)][string]$ComparePath,
        [Parameter(Mandatory)][string]$Cutoff
    )
    Invoke-StorageCommand @("--verify-db", $CopyPath, "--compare-db", $ComparePath, "--cutoff", $Cutoff) | Out-Null
}

function Swap-Database {
    param(
        [Parameter(Mandatory)][string]$CompactedPath,
        [Parameter(Mandatory)][string]$Cutoff
    )

    Invoke-Step "Preparing to swap compacted database back to T:." {
        Assert-McpStopped
        Assert-DbUnlocked

        $stateDir = Split-Path -Parent $script:DataDb
        $verifyCopy = Join-Path $stateDir "data_compacted_verify.db"
        $backup = Join-Path $stateDir ("data.db.pre-reclaim-{0}.bak" -f (Get-Date -Format "yyyyMMdd-HHmmss"))
        $compactedSize = (Get-Item -LiteralPath $CompactedPath).Length
        $freeBytes = Get-TDriveFreeBytes
        $bufferBytes = 5GB

        if ($freeBytes -gt ($compactedSize + $bufferBytes)) {
            Write-Log "Using copy-then-swap path. T: has enough free space for the compacted DB plus buffer."
            if (Test-Path -LiteralPath $verifyCopy) {
                throw "T: verify copy already exists: $verifyCopy"
            }
            Copy-Item -LiteralPath $CompactedPath -Destination $verifyCopy
            Verify-DatabaseCopy -CopyPath $verifyCopy -ComparePath $script:DataDb -Cutoff $Cutoff

            Assert-McpStopped
            Assert-DbUnlocked
            Move-Item -LiteralPath $script:DataDb -Destination $backup
            Remove-DbSidecars
            try {
                Move-Item -LiteralPath $verifyCopy -Destination $script:DataDb
                Invoke-StorageCommand @("--status", "--cutoff", $Cutoff) | Out-Null
                Remove-Item -LiteralPath $backup -Force
                Write-Log "Swap complete; removed old database backup $backup."
            } catch {
                Write-Log "Swap failed; attempting rollback from $backup."
                if ((Test-Path -LiteralPath $backup) -and -not (Test-Path -LiteralPath $script:DataDb)) {
                    Move-Item -LiteralPath $backup -Destination $script:DataDb
                }
                throw
            }
        } else {
            Write-Log "Using fallback delete-then-move path because T: does not have enough free space for copy-then-swap."
            Verify-DatabaseCopy -CopyPath $CompactedPath -ComparePath $script:DataDb -Cutoff $Cutoff
            Assert-McpStopped
            Assert-DbUnlocked
            Remove-Item -LiteralPath $script:DataDb -Force
            Remove-DbSidecars
            Move-Item -LiteralPath $CompactedPath -Destination $script:DataDb
            Invoke-StorageCommand @("--status", "--cutoff", $Cutoff) | Out-Null
        }
    } -Destructive
}

function Invoke-Reclaim {
    $cutoff = Get-ArchiveCutoff
    Write-Log "Mode=$Mode Confirm=$Confirm DryRun=$DryRun WhatIf=$WhatIf Cutoff=$cutoff"
    Write-Log ("T: free before run: {0:N2} GB" -f ((Get-TDriveFreeBytes) / 1GB))
    Assert-StorageExe

    if ($Mode -eq "PrepareArchiveDrive") {
        Prepare-ArchiveDrive
        return
    }

    if ($Mode -eq "VerifyOnly") {
        $compacted = $script:ArchiveStateDir + "\data_compacted.db"
        Verify-DatabaseCopy -CopyPath $compacted -ComparePath $script:DataDb -Cutoff $cutoff
        return
    }

    Prepare-ArchiveDrive

    if ($Mode -eq "CompactOnly" -and $MinFreelistGb -gt 0) {
        if ($AbortIfMcpRunning -and (Get-Process -Name $script:McpProcessName -ErrorAction SilentlyContinue)) {
            Write-Log "$script:McpProcessName is running; aborting scheduled compaction."
            return
        }
        $freelistGb = Get-FreelistGb
        if ($freelistGb -lt $MinFreelistGb) {
            Write-Log "SQLite freelist is $freelistGb GB, below threshold $MinFreelistGb GB; skipping compaction."
            return
        }
    }

    Stop-Mcp
    Assert-DbUnlocked

    if ($Mode -eq "FullReclaim") {
        Update-ColdArchiveConfig
        Move-ExistingArchives
        Invoke-ArchiveOldTicks -Cutoff $cutoff
    }

    $compacted = Invoke-CompactIntoArchive -Cutoff $cutoff
    if (-not ($DryRun -or $WhatIf)) {
        Verify-DatabaseCopy -CopyPath $compacted -ComparePath $script:DataDb -Cutoff $cutoff
    }
    Swap-Database -CompactedPath $compacted -Cutoff $cutoff
    Write-Log ("T: free after run: {0:N2} GB" -f ((Get-TDriveFreeBytes) / 1GB))
}

Initialize-Logging
try {
    if ($Mode -ne "VerifyOnly" -and -not ($DryRun -or $WhatIf)) {
        Require-Admin
        if (-not $Confirm) {
            throw "Mode $Mode requires explicit -Confirm. Use -DryRun or -WhatIf to inspect without changes."
        }
    }
    Invoke-Reclaim
    Write-Log "Reclaim-Storage.ps1 completed."
} catch {
    Write-Log "ERROR: $($_.Exception.Message)"
    throw
}
