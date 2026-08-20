---
'@firebase/database': patch
'firebase': patch
---

Ingest full-root server pushes in yielded slices instead of one monolithic
main-thread task. A cold boot's initial payload — or the server replacing a
restored base it could not range-merge — previously decoded, diffed, and
applied the entire root synchronously (a multi-second block plus an
allocation spike on large workspaces, exactly when mobile WebKit is
quickest to kill the page). The decode now runs under a per-key slice
budget with macrotask yields, unchanged children are grafted from the live
base by identity so the single atomic SyncTree overwrite diffs only the
changed portion, and listeners still observe one coherent root transition
(one event batch, one transaction rerun — the wire contract).

Deferred wire operations (data, range merges, listen completions, and
onDisconnect runs frozen at their own disconnect) drain through one
repo-level ordered queue, preserving exact arrival order across roots and
kinds. Account switches drop queued account-bound operations and cancel
in-flight decodes; teardown can never strand or double-apply a queued
operation.

`ListenOutcomeReason` gains two literals: `'auth-timeout'` (identity
hydration outlived the restore backstop and the listen fell open cold) and
`'partial-descendants'` (a filtered descendant query's window blocked the
restored-base graft), so forced-cold boots are attributable in telemetry.
