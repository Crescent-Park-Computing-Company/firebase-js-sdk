---
'@firebase/database': patch
---

Gentle write flush: the persistence flush no longer stalls the main thread on
large roots. Stable-range planning (the full-leaf rewalk on a first
generation or a full-reload baseline) now runs in bounded main-thread slices
via a resumable walker; range staging batches are cut by canonical-text bytes
and yield through unclamped macrotasks; the manifest's estimatedBytes comes
from the planned range sizes instead of a second full-tree walk; a baseline
that shares no identity with the live tree skips the pointless identity diff;
and a root with no stored generation flushes on a shorter first-generation
window so short mobile sessions produce a cache before they end (breaking the
cold-reload loop: no cache -> full download -> death before flush -> no cache).
