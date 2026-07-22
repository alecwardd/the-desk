# Shared exit-code convention for The Desk ops scripts.
# Dot-source from other scripts:
#   . (Join-Path $PSScriptRoot "Desk-ExitCodes.ps1")
#
#   0  success
#   2  deferred (MCP writer active / maintenance blocked)
#   3  config error
#   4  integrity failure
#   5  storage failure
#   1  general failure

$script:EXIT_SUCCESS = 0
$script:EXIT_GENERAL = 1
$script:EXIT_DEFERRED = 2
$script:EXIT_CONFIG = 3
$script:EXIT_INTEGRITY = 4
$script:EXIT_STORAGE = 5
