---
'@firebase/database': patch
'firebase': patch
---

Fix a multi-tab flush thrash in the fork's IndexedDB persistence: concurrent
tabs no longer leapfrog each other's manifest revisions (full-tree re-stages
in every tab every write window). A single Web Lock per database prefix
elects one writer per origin, holder liveness is proven by a localStorage
heartbeat, and a queued follower steals the lock from a suspended holder.
CAS-conflict recovery now adopts the committed manifest without reading or
decoding ranges, and conflict retries are debounced into the ordinary write
window. Destructive cleanup (eviction, corrupt-record removal, invalidation)
is ownership- or revision-guarded so a follower can never delete a
generation the current writer just committed.
