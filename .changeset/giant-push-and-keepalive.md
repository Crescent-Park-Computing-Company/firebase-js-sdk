---
'@firebase/database': patch
---

Two main-thread fixes for giant server messages. (1) The sliced ingest pump
now engages by PAYLOAD SIZE as well as path registration: an untagged,
non-merge, children-shaped push whose message exceeded 1 MiB of wire bytes
is sliced even when its path is not a registered persistent root — giant
listen answers for unregistered listens previously ran the monolithic
decode/apply on the socket callback. (2) The websocket keepalive no longer
tears down and re-arms its interval timer on every frame; activity is
tracked by timestamp and a periodic tick sends the no-op ping after a full
quiet interval, removing timer-churn that consumed ~38% of the receive
window on multi-MB messages.
