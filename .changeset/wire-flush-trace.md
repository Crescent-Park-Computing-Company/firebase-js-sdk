---
'@firebase/database': patch
---

Add `wire-message` and `flush` persistence trace events: every server data
operation reports its path, wire bytes, kind, and ingestion route
(sync/queued/sliced) at arrival, and every committed persistence generation
reports range reuse counters plus baseline identity sharing — production
observability for giant-message attribution and flush-baseline memory
retention, through the existing `__firebaseDatabasePersistenceTrace` sink.
