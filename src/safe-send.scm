; safe-send.scm — Erlang-semantics send: a message to a dead/unknown pid is a
; NO-OP, never an error (cw-2au). Field failure: one range-worker crash made the
; shard raise on its reply send ("actor <n> not found"), killing the shard, then
; every grpc-kv worker's ask-shard raised the same way -> whole serving plane
; down while the process stayed up. Include ONCE at the top of each actor
; ENTRYPOINT source (never in shared includes — a second (define %send send)
; would capture the shim and loop).
(define %send send)
(define (send p m) (guard (e (#t #f)) (%send p m)))
