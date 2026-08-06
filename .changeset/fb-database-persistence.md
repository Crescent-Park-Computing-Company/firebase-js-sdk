---
'@firebase/database': minor
'firebase': minor
---

Add SDK-owned IndexedDB persistence to the Realtime Database fork through the
internal hooks `_setPersistenceEnabled`, `_setPersistencePath`,
`_setPersistenceAuthScope`, `_getPersistedValue`, and `_onListenOutcome`.
Selected default-listen roots restore cached events before server
certification; unchanged trees complete without a full download and changed
trees reconcile through compound-hash range merges. This also ports the
compound-hash listen and range-merge protocol used by the Android and iOS
SDKs.
