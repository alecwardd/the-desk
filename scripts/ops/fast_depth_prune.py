"""Rowid-based prune of old depth_events — the one-time reclaim's heavy step.

Why not `the-desk-storage --prune-depth`? That deletes by `trading_day`. After
>1B rows are deleted the query planner abandons the trading_day index and
full-scans the 629 GB table (~8k rows/s). depth_events.rowid (the AUTOINCREMENT
id) is monotonic with trading_day because ingestion is chronological, so we:

  1. binary-search the rowid boundary for the retention cutoff (instant, primary-
     key lookups — no scan),
  2. delete `rowid < threshold` in chunks (primary-key driven; no trading_day scan;
     secondary-index maintenance stays localized at the low end of each index).

Idempotent / resumable: re-running recomputes the same threshold and continues.
The Sierra `.depth` files remain the durable source, so pruned rows are
re-ingestable. Reclaim the freed pages afterward with `the-desk-storage
--compact-into`.

Usage: python fast_depth_prune.py [YYYY-MM-DD cutoff, default = today-7]
"""
import sqlite3
import sys
import time
from datetime import date, timedelta

DB = r"C:\Users\alecw\.the-desk\data.db"
cutoff = sys.argv[1] if len(sys.argv) > 1 else (date.today() - timedelta(days=7)).isoformat()


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


con = sqlite3.connect(DB, timeout=300, isolation_level=None)  # autocommit
con.execute("PRAGMA temp_store=FILE")
try:
    con.execute("PRAGMA temp_store_directory='X:\\TheDesk\\temp'")
except sqlite3.Error:
    pass
con.execute("PRAGMA wal_autocheckpoint=20000")  # ~80 MB, bounds the WAL on a near-full drive

minr = con.execute("SELECT MIN(rowid) FROM depth_events").fetchone()[0]
maxr = con.execute("SELECT MAX(rowid) FROM depth_events").fetchone()[0]
if minr is None:
    log("depth_events empty; nothing to do")
    sys.exit(0)

# binary search: first rowid whose trading_day >= cutoff
lo, hi = minr, maxr
while lo < hi:
    mid = (lo + hi) // 2
    row = con.execute(
        "SELECT rowid,trading_day FROM depth_events WHERE rowid>=? ORDER BY rowid LIMIT 1", (mid,)
    ).fetchone()
    if row is None:
        hi = mid
        continue
    rid, td = row
    if td is None or td < cutoff:
        lo = rid + 1
    else:
        hi = rid
threshold = lo
below = con.execute(
    "SELECT trading_day FROM depth_events WHERE rowid<? ORDER BY rowid DESC LIMIT 1", (threshold,)
).fetchone()
at = con.execute(
    "SELECT trading_day FROM depth_events WHERE rowid>=? ORDER BY rowid LIMIT 1", (threshold,)
).fetchone()
log(f"cutoff={cutoff} threshold_rowid={threshold} (last_below={below}, first_at={at}) min={minr} max={maxr}")
if threshold <= minr:
    log("no rows older than cutoff; done")
    sys.exit(0)

BATCH = 200_000
deleted = 0
since_ckpt = 0
t0 = time.time()
while True:
    n = con.execute(
        "DELETE FROM depth_events WHERE rowid IN (SELECT rowid FROM depth_events WHERE rowid < ? LIMIT ?)",
        (threshold, BATCH),
    ).rowcount
    deleted += n
    since_ckpt += n
    if since_ckpt >= 4_000_000:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        since_ckpt = 0
        el = max(1.0, time.time() - t0)
        log(f"deleted {deleted} in {int(el)}s ({deleted/el/1000:.0f}k rows/s)")
    if n < BATCH:
        break
con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
log(f"DONE: deleted {deleted} rows older than {cutoff} in {int(time.time()-t0)}s. Run --compact-into to reclaim.")
