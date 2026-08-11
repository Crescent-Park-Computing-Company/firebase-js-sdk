---
'@firebase/database': minor
'firebase': minor
---

Add SDK-owned IndexedDB persistence to the Realtime Database fork. Enable it
with `setPersistenceEnabled(db, true)` and bind the identity with
`setPersistenceAuthScope(db, uid)`; select roots per listener with the
`{ persistent: true }` listen option. Selected default-listen roots restore
cached events before server certification; unchanged trees complete without a
full download and changed trees reconcile through compound-hash range merges.
`getPersistedValue()` reads the exact persisted root for pre-auth optimistic
paints, and `onListenOutcome()` reports the restore/cold/fallback mode and the
final certification per root. Persistence stores one tiny manifest plus
immutable fixed-target range records, rewrites only dirty ranges, guards
cross-tab commits by manifest revision, and garbage-collects retired records.
This also ports the compound-hash listen and range-merge protocol used by the
Android and iOS SDKs.
