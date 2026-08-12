<#
.SYNOPSIS
  Start / stop / watchdog the-desk-engine for Globex overnight coverage (SIL-M2a).

.DESCRIPTION
  Keeps the headless engine host running whenever Sierra is expected to record.
  Closing Cursor / the-desk-mcp must not stop ingest — this process owns it when
  [sil].engine_mode = "external".

  Trust Ceiling stays L3: this script never places orders.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet("Watchdog", "Start", "Stop", "Status")]
    [string]$Action,

    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,

    [string]$EngineExe = ""
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "Desk-ExitCodes.ps1")

function Resolve-EngineExe {
    if ($EngineExe -and (Test-Path -LiteralPath $EngineExe)) { return $EngineExe }
    $candidates = @(
        (Join-Path $RepoRoot "target_alt\release\the-desk-engine.exe"),
        (Join-Path $RepoRoot "target\release\the-desk-engine.exe"),
        (Join-Path $RepoRoot "target\release\the-desk-engine")
    )
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) { return $c }
    }
    return $null
}

function Test-EngineRunning {
    return [bool](Get-Process -Name "the-desk-engine" -ErrorAction SilentlyContinue)
}

switch ($Action) {
    "Status" {
        if (Test-EngineRunning) {
            Write-Output "the-desk-engine is running"
            exit $script:EXIT_SUCCESS
        }
        Write-Output "the-desk-engine is not running"
        exit $script:EXIT_SUCCESS
    }
    "Stop" {
        Get-Process -Name "the-desk-engine" -ErrorAction SilentlyContinue | Stop-Process -Force
        exit $script:EXIT_SUCCESS
    }
    "Start" {
        if (Test-EngineRunning) { exit $script:EXIT_SUCCESS }
        $exe = Resolve-EngineExe
        if (-not $exe) {
            Write-Error "the-desk-engine binary not found under $RepoRoot"
            exit $script:EXIT_CONFIG
        }
        Start-Process -FilePath $exe -WorkingDirectory $RepoRoot -WindowStyle Hidden
        exit $script:EXIT_SUCCESS
    }
    "Watchdog" {
        # Align with Sierra recording window conceptually: if engine is down, start it.
        # Globex overnight coverage requires this task to keep firing while Sierra records.
        if (Test-EngineRunning) { exit $script:EXIT_SUCCESS }
        $exe = Resolve-EngineExe
        if (-not $exe) {
            Write-Error "the-desk-engine binary not found under $RepoRoot"
            exit $script:EXIT_CONFIG
        }
        Start-Process -FilePath $exe -WorkingDirectory $RepoRoot -WindowStyle Hidden
        exit $script:EXIT_SUCCESS
    }
}
