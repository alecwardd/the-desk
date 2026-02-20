---
name: dtc-check
description: Verify DTC connection health and data flow. USE WHEN troubleshooting connectivity to Sierra Chart, verifying data is flowing, or after modifying the DTC client.
---

# /dtc-check

Check DTC connection status and verify data flow.

## Steps

1. Check if the DTC client process is running and connected:
   ```bash
   # Check connection status via Tauri command
   # This will be implemented as a Tauri command that returns DtcStatus
   ```

2. Verify data is flowing:
   - Last trade received timestamp (should be within last few seconds during RTH)
   - Messages per second rate
   - Any parsing errors in the DTC log

3. If disconnected, check:
   - Is Sierra Chart running?
   - Is the DTC server enabled in SC settings?
   - Is the configured IP:port correct (check `~/.the-desk/config.toml`)?
   - Any error messages in the connection log?

4. If connected but no data:
   - Is the symbol correct? (Check against SC's symbol naming)
   - Is the market open? (RTH: 9:30-4:15 ET, Globex: 6PM-5PM ET)
   - Is Rithmic connected in SC? (Check SC's connection status)

5. Report connection status, data rate, and any issues found.

## Quick Diagnostic

```bash
# Check if SC DTC port is listening
# On Windows:
netstat -an | findstr 11099

# Test raw TCP connection
# (from the mock DTC server or a simple TCP test)
```
