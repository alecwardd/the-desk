# IDEA-034 — Bucket-Stats Provenance Verification

**Date:** 2026-08-11
**Mode:** RESEARCH — bucket-stats artifact gate only
**Verdict:** `ACCEPTED`

## Scope

This record closes the provenance correction required during review of
`T:\TheDesk\temp\backtests\idea-034-bucket-stats\run-20260806T105917Z`.
The correction was limited to a detached clean build of `b63e83a`, an isolated
replay with the original campaign inputs/configuration/contract windows, and a
comparison of approved provenance and core metrics.

This is **not** calendar extraction, an IDEA-034 Stage 1 pass, Stage 2,
hypothesis registration, or live/trading authorization.

## Result

| Check | Reviewed artifact | Clean verification | Verdict |
|-------|-------------------|--------------------|---------|
| In-scope buckets | 267 | 267 | Match |
| Eligible bucket rows | 34,977 | 34,977 | Match |
| Eligible N, min / median | 131 / 131 | 131 / 131 | Match |
| Clean-subset N, min / median | 19 / 19 | 19 / 19 | Match; underpowered but labeled |
| Exclusions | DST 16; holiday/early-close 8; weekend 13; material gap 1 (`2026-06-26`) | Same | Match |
| Rollover crossovers | H6→M6 `2026-03-16`; M6→U6 `2026-06-15` | Same | Match |
| Warnings | 0 | 0 | Match |
| Date/bucket keys | 48,384 rows; 48,384 distinct; 0 duplicate groups | Same | Match |

The five deterministic evidence files were byte-identical:

| File | SHA256 |
|------|--------|
| `bucket_stats.jsonl` | `a619aafac82c630e3830bffd706482291c8ff5cae0a1d235d40de7843b82b112` |
| `coverage.json` | `cc9f646e3f25559e8ab2cc049d9489c3f68ab5c3d0f4c5153bdc4b55e2b95925` |
| `exclusions.json` | `bd3f32725e01208812d8c75f0fc6527a38e4d067a27abd10b88a296b7d459a4c` |
| `rollover_evidence.json` | `3cdd4bcc31c57837ae3edd188b3841b844fb23a84a5ac52f2fb42597e3d6574e` |
| `summary.json` | `a274bc44674d72ca9f4ad806bc6072fbddde8771d4a305ba87821cede4d98415` |

## Clean provenance

- Commit: `b63e83a1f6ed525380093545624360cc790ceb3f`
- Git dirty: `false`
- Binary SHA256: `16523038aefd8ba75cf79bbeaa7a9f7531370ea0a78d19b1a1d07d95cb03ea6b`
- `src/bin/the-desk-idea034-bucket-stats.rs`: `d16eab9549393af97168f125ed7b91c39784941ecad5b86eae8e537b3fa2a7c1`
- `src/feed/scid_reader.rs`: `826b99394cbd59d6dfb436479f59135b9199ee19dce6f9d1e063aad6ee5d8af0`
- `src/lib.rs`: `8997181010ff513912ee4357129cd1453287f783ec543c663a51dc46b0d5d64b`
- `src/research/ib_campaign.rs`: `fd98f680ff4ca61332d6e7384c018cf6005e1745af2e06ea146f457cf86ba3a3`
- `src/research/idea034_bucket_stats.rs`: `eea8f57fd673d50b2e04e9a8ee9304e81843e0949fa6e0cc06fdb11361e453c7`

The build used the detached clean worktree, not the dirty main checkout. The
replay wrote to a new isolated database beneath the campaign root; it did not
target the live database. The temporary worktree/build/replay were removed
only during the separately authorized workblock cleanup after this evidence
was captured. The original reviewed run remains the retained artifact.

## Decision and remaining gates

`ACCEPTED` applies only to the bucket-stats artifact gate. The clean-subset
fold remains directional-only at N=19 and does not block the primary N=131
coverage result. Calendar/event-rate extraction requires explicit approval
and a separate Stage 1 verdict. Stage 2, signals, live/trading integration,
and Git publication remain separately gated.
