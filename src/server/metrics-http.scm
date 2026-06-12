; server/metrics-http.scm — the crab-watchstore endpoint HTTP surface (cw-u4a.33).
;
; A DEDICATED HTTP/1.1 listener that serves /health, /version and /metrics, exactly as
; etcd's `--listen-metrics-urls` does on a SEPARATE port from the client API.  Why a separate
; port: the client port runs the Rust h2c gRPC transport (cw-u4a.20), which hardcodes
; content-type application/grpc and cannot serve plain HTTP — so the HTTP surface MUST live on
; its own listener (node-cluster.scm derives it as client-port + 10000 by default, overridable
; with --metrics-port).
;
;   (spawn-source-dedicated "(include \"src/server/metrics-http.scm\")" 'metrics-http-main
;                           SHARD-PID ME-STRING HOST PORT)
;     SHARD-PID  : this node's shard-0 replica PID (for the read seam)
;     ME-STRING  : this node's name (symbol->string) — for etcd_server_is_leader
;     HOST PORT  : the metrics listener bind address
;
; DEDICATED thread (NOT a shared green worker): tcp-accept BLOCKS with no cooperative hook, and
; each request does a blocking shard round-trip (send + raw-receive) — both would freeze the
; shared green pool (green-threads INV-2/3), same rationale as the shard / poller / grpc-kv
; handler.  One request per connection (Connection: close, HTTP/1.0-style) keeps the parser
; trivial and dodges keep-alive / partial-read edge cases.
;
; All values are REAL per-node scalars read from the shard's .32 status seam (cw-u4a.33 appended
; the live key-count): one round-trip per request yields rev / term / commit / applied / db-size
; / leader / key-count.  The seam is endpoint-local + un-gated, so every node answers honestly.

(include "src/encoding.scm")   ; subbv (+ confirms string->utf8 / utf8->string / bytevector-u8-ref)

; ===========================================================================
; HTTP request reading + parsing (binary-safe; we only ever need the request LINE)
; ===========================================================================

; Does the buffer contain the end-of-headers marker CR LF CR LF (13 10 13 10)?
(define (bytes-headers-end? bv)
  (let ((n (bytevector-length bv)))
    (let scan ((i 0))
      (cond ((> (+ i 3) (- n 1)) #f)
            ((and (= (bytevector-u8-ref bv i)       13)
                  (= (bytevector-u8-ref bv (+ i 1)) 10)
                  (= (bytevector-u8-ref bv (+ i 2)) 13)
                  (= (bytevector-u8-ref bv (+ i 3)) 10)) #t)
            (else (scan (+ i 1)))))))

; Read the request off the socket.  We accumulate until we see the \r\n\r\n header terminator
; (the normal path: a real client — curl / grpc_health_probe / k8s — sends the whole request,
; usually in ONE read), OR the peer half-closes (tcp-recv returns empty = EOF), OR a small cap
; trips.  We only ever parse the request LINE, so this never blocks waiting for a body, and it
; is robust to a client that sends just the request line then closes (EOF stops the loop; the
; parser tolerates a header-less buffer).
(define HTTP-READ-CAP 65536)
(define (http-read-request conn)
  (let loop ((acc (make-bytevector 0 0)) (iters 0))
    (let* ((chunk (tcp-recv conn 4096))
           (acc2  (bytevector-append acc chunk)))
      (cond
        ((= (bytevector-length chunk) 0) acc2)            ; EOF / clean half-close
        ((bytes-headers-end? acc2) acc2)                  ; full request headers received
        ((>= (bytevector-length acc2) HTTP-READ-CAP) acc2); oversized — stop, parse what we have
        ((>= iters 64) acc2)                              ; pathological dribble — bounded
        (else (loop acc2 (+ iters 1)))))))

; The request line is the bytes up to the first LF; strip a trailing CR; decode as UTF-8 (the
; request line is ASCII).  Returns "" if the buffer is empty / undecodable.
(define (http-request-line bv)
  (let* ((n   (bytevector-length bv))
         (eol (let scan ((i 0))
                (cond ((>= i n) n)
                      ((= (bytevector-u8-ref bv i) 10) i)
                      (else (scan (+ i 1))))))
         (e2  (if (and (> eol 0) (= (bytevector-u8-ref bv (- eol 1)) 13)) (- eol 1) eol)))
    (guard (err (#t "")) (utf8->string (subbv bv 0 e2)))))

; The request-target (2nd whitespace token) of a "METHOD SP TARGET SP VERSION" line.  Tolerant:
; a missing version ("GET /metrics") still yields the target; a line with no space yields "".
(define (http-target line)
  (let* ((len (string-length line))
         (sp1 (let scan ((i 0))
                (cond ((>= i len) #f)
                      ((char=? (string-ref line i) #\space) i)
                      (else (scan (+ i 1)))))))
    (if (not sp1) ""
        (let* ((start (+ sp1 1))
               (sp2 (let scan ((i start))
                      (cond ((>= i len) len)
                            ((char=? (string-ref line i) #\space) i)
                            (else (scan (+ i 1)))))))
          (if (>= start len) "" (substring line start sp2))))))

; Drop a ?query suffix so /metrics?foo=bar routes as /metrics.
(define (http-path target)
  (let* ((len (string-length target))
         (q   (let scan ((i 0))
                (cond ((>= i len) #f)
                      ((char=? (string-ref target i) #\?) i)
                      (else (scan (+ i 1)))))))
    (if q (substring target 0 q) target)))

; ===========================================================================
; HTTP response (status line + Content-Type + Content-Length + Connection: close + body)
; ===========================================================================
(define (http-send conn code reason ctype body-str)
  (let* ((body (string->utf8 body-str))
         (head (string-append
                 "HTTP/1.1 " (number->string code) " " reason "\r\n"
                 "Content-Type: " ctype "\r\n"
                 "Content-Length: " (number->string (bytevector-length body)) "\r\n"
                 "Connection: close\r\n"
                 "\r\n")))
    (tcp-send conn (bytevector-append (string->utf8 head) body))))

; ===========================================================================
; shard read seam — one .32 status round-trip per request (this actor's PID is the reply-pid;
; nothing else messages this actor, so raw-receive yields exactly the status-ok reply).
;   -> (status-ok rev term commit applied db-size leader key-count) | #f
; ===========================================================================
(define (metrics-shard-status shard-pid)
  (send shard-pid (list 'status (self)))
  (let wait ()
    (let ((r (raw-receive)))
      (cond ((and (pair? r) (eq? (car r) 'status-ok)) r)
            ((pair? r) (wait))   ; ignore any stray frame
            (else (wait))))))

; status-ok field accessors (graceful on a #f / malformed reply -> zeros / no leader).
(define (st-ok?    r) (and (pair? r) (eq? (car r) 'status-ok)))
(define (st-rev    r) (if (st-ok? r) (list-ref r 1) 0))
(define (st-term   r) (if (st-ok? r) (list-ref r 2) 0))
(define (st-commit r) (if (st-ok? r) (list-ref r 3) 0))
(define (st-applied r)(if (st-ok? r) (list-ref r 4) 0))
(define (st-dbsize r) (if (st-ok? r) (list-ref r 5) 0))
(define (st-leader r) (and (st-ok? r) (list-ref r 6)))
(define (st-keys   r) (if (st-ok? r) (list-ref r 7) 0))

; READY = has-leader AND initialized (applied >= commit) — same readiness as the gRPC Check.
(define (st-ready? r) (and (st-ok? r) (st-leader r) (>= (st-applied r) (st-commit r))))

; ===========================================================================
; route handlers
; ===========================================================================

; GET /health — etcd-faithful {"health":"true"|"false","reason":...}; 200 healthy / 503 not.
(define (route-health conn r)
  (if (st-ready? r)
      (http-send conn 200 "OK" "application/json"
                 "{\"health\":\"true\",\"reason\":\"\"}")
      (http-send conn 503 "Service Unavailable" "application/json"
                 "{\"health\":\"false\",\"reason\":\"NO LEADER\"}")))

; GET /version — server version + cluster version.  Cluster version = the agreed cluster-wide
; minimum; we report the same constant 3.6.0 (the static cluster runs one version), DOCUMENTED.
(define (route-version conn)
  (http-send conn 200 "OK" "application/json"
             "{\"etcdserver\":\"3.6.0\",\"etcdcluster\":\"3.6.0\"}"))

; one Prometheus gauge: # HELP / # TYPE / value (all the gauges here are integers).
(define (gauge name help value)
  (string-append "# HELP " name " " help "\n"
                 "# TYPE " name " gauge\n"
                 name " " (number->string value) "\n"))

; GET /metrics — Prometheus exposition (text/plain; version=0.0.4) of REAL per-node gauges.
; member locality (cw-lkq.7): "region[/zone]", set by metrics-http-main from the
; launcher's --locality / cluster-spec region field; "" = unlabelled member.
(define metrics-locality "")
(define (locality-metric me-str)
  (if (= 0 (string-length metrics-locality)) ""
      (let* ((parts (let loop ((i 0))
                      (cond ((= i (string-length metrics-locality)) (list metrics-locality ""))
                            ((char=? (string-ref metrics-locality i) #\/)
                             (list (substring metrics-locality 0 i)
                                   (substring metrics-locality (+ i 1)
                                              (string-length metrics-locality))))
                            (else (loop (+ i 1)))))))
        (string-append "# HELP crabwatchstore_member_locality Member locality labels.\n"
                       "# TYPE crabwatchstore_member_locality gauge\n"
                       "crabwatchstore_member_locality{member=\"" me-str
                       "\",region=\"" (car parts) "\",zone=\"" (cadr parts) "\"} 1\n"))))

(define (route-metrics conn me-str r)
  (let* ((leader     (st-leader r))
         (has-leader (if leader 1 0))
         (is-leader  (if (and leader (string=? (symbol->string leader) me-str)) 1 0))
         (body (string-append
                 (gauge "etcd_server_has_leader"
                        "Whether or not a leader exists. 1 is existence, 0 is not." has-leader)
                 (gauge "etcd_server_is_leader"
                        "Whether or not this member is a leader. 1 if is, 0 otherwise." is-leader)
                 (gauge "etcd_server_raft_term"
                        "The current raft term of the server." (st-term r))
                 (gauge "etcd_server_raft_index"
                        "The current raft committed index of the server." (st-commit r))
                 (gauge "etcd_server_raft_applied_index"
                        "The current raft applied index of the server." (st-applied r))
                 (gauge "etcd_debugging_mvcc_current_revision"
                        "The current revision of the store." (st-rev r))
                 (gauge "etcd_mvcc_db_total_size_in_bytes"
                        "Total size of the underlying database logically in bytes." (st-dbsize r))
                 (gauge "etcd_debugging_mvcc_keys_total"
                        "Total number of keys in the store." (st-keys r))
                 (locality-metric me-str)
                 (gauge "up" "1 if the metrics endpoint is serving." 1))))
    (http-send conn 200 "OK" "text/plain; version=0.0.4" body)))

; ===========================================================================
; per-connection handling + accept loop
; ===========================================================================
(define (metrics-handle-conn shard-pid me-str conn)
  (let* ((req  (http-read-request conn))
         (path (http-path (http-target (http-request-line req)))))
    (cond
      ((string=? path "/health")  (route-health  conn (metrics-shard-status shard-pid)))
      ((string=? path "/metrics") (route-metrics conn me-str (metrics-shard-status shard-pid)))
      ((string=? path "/version") (route-version conn))
      (else (http-send conn 404 "Not Found" "text/plain; charset=utf-8" "404 page not found\n")))))

(define (metrics-http-main shard-pid me-str host port . rest)
  ; optional 5th arg (cw-lkq.7): the member's locality string "region[/zone]".
  ; (a define so it precedes the body's other defines — set! is in expression
  ; position there and Scheme bodies require defines first.)
  (define locality-init
    (if (and (pair? rest) (string? (car rest)))
        (set! metrics-locality (car rest))))
  ; A bind failure must NOT crash the node — log + exit this actor cleanly (the node keeps
  ; serving gRPC).  spawn-source-dedicated isolates this thread; the guard keeps stderr clean.
  (define listener
    (guard (e (#t (display "metrics-http: tcp-listen failed on ") (display host) (display ":")
                  (display port) (display " — metrics disabled on this node") (newline)
                  #f))
      (tcp-listen host port)))
  (when listener
    (display "node ") (display me-str) (display ": metrics/health HTTP serving on ")
    (display host) (display ":") (display port) (newline)
    ; One request per connection.  A bad connection is contained (close + continue); a dead
    ; listener exits the loop (the actor ends, node unaffected).
    (let accept-loop ()
      (let ((conn (guard (e (#t #f)) (tcp-accept listener))))
        (when conn
          (guard (e (#t #f)) (metrics-handle-conn shard-pid me-str conn))
          (guard (e (#t #f)) (tcp-close conn))
          (accept-loop))))))
