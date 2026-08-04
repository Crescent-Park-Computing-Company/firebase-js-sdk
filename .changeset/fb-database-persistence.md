---
'@firebase/database': minor
'firebase': minor
---

Add client-side persistence of the Realtime Database server cache
(`setPersistenceEnabled`): listened roots are stored in IndexedDB and
restored on the next startup, where they raise cached events immediately and
revalidate with the server via listen hashes — an unchanged tree completes
with no data download, a changed one is reconciled with range-merge deltas.
Also implements the compound-hash listen and range-merge protocol support
the Android and iOS SDKs already have, plus a server-cache seeding seam and
`getPersistedValue` for reading the persisted cache without a listener.
