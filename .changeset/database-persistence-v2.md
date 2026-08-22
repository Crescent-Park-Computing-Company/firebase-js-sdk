---
'@firebase/database': minor
'firebase': minor
---

Client-side server-cache persistence (v2) for web: path-keyed row storage in
IndexedDB (Android storage parity), write-through-the-op flushes with no tree
diffing, compound listen hashes computed at boot/reconnect from stored rows in
a Web Worker (sliced main-thread fallback), restore→graft→apply→hash→listen
boot flow, Web Locks writer lease, age sweep. Includes the compound-hash /
range-merge wire protocol, sliced full-root push ingestion (size-based
eligibility), sliced peek materialization, and O(1) keepalive activity
tracking. New API: setPersistenceEnabled, setPersistenceAuthScope,
getPersistedValue, onListenOutcome, consumePersistedMaterialization, and the
`persistent` listen option.
