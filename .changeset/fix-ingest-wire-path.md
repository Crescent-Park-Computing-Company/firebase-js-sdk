---
'@firebase/database': patch
---

Fix the sliced full-root push ingestion never engaging in production: the
wire delivers server-form path strings (no leading slash), while persistence
roots, ingest gates, and queued operations are keyed by canonical
Path.toString() form. Wire paths are now canonicalized at the repo boundary
(repoOnDataUpdate / repoOnRangeMergeUpdate), so full-root pushes at
persistent roots divert to the sliced ingest pump instead of the monolithic
decode/apply.
