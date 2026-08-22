---
'@firebase/database': patch
---

Release the flush baseline tree on a wholesale (divorced) server replace and
flush pending persistence writes on pagehide/visibility-hidden. A fallback
resend, giant sliced overwrite, or whole-root range merge leaves
`lastFlush_.rootNode` sharing no identity with the live tree — retaining a
second complete in-memory copy that a non-writer tab would keep forever; the
baseline tree is now dropped immediately (manifest-only adopted shape, CAS
preserved). The new lifecycle flush commits the pending write window when the
page hides, so the next boot restores a fresher tree and the restored listen's
server delta shrinks.
