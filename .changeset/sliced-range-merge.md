---
'@firebase/database': patch
---

Slice giant range-merge ingestion. A stale restored listen's range resend
can approach the whole root; the synchronous path decoded every range with
nodeFromJSON and folded each merge over the view's server cache inside the
socket message callback — a 10-second main-thread stall with nested full
GCs on mobile Safari. An untagged range merge whose message exceeded 1 MiB
of wire bytes now runs through the sliced ingest pump: budgeted decode of
each range's update tree, off-tree folds with yields between ranges, and
ONE atomic SyncTree overwrite at the end. Small and tagged merges keep the
synchronous path.
