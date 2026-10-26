(require-builtin steel/tcp)
(require-builtin steel/json)
(require-builtin steel/hash)
(require "collections.scm")

(define (read-events reader remaining)
  (if (= remaining 0)
    #t
    (let ((event (string->jsexpr (read-line reader))))
      (if (equal? (hash-> event status) "ok")
        (begin
          (display (hash-> event key))
          (display ": ")
          (display (hash-> event value))
          (display " ")
          (display (hash-> event unit))
          (display " at ")
          (display (hash-> event completed_at_unix_ms))
          (newline))
        (begin
          (display "Measurement error: ")
          (display (hash-> event error))
          (newline)))

      (read-events reader (- remaining 1)))))

(let* ((connection (tcp-connect "127.0.0.1:7240"))
       (writer (tcp-stream-writer connection))
       (reader (tcp-stream-buffered-reader connection)))

  (display "{\"id\":2,\"op\":\"subscribe\",\"key\":\"sample_temperature\"}\n"
    writer)
  (flush-output-port writer)

  (let ((ack (string->jsexpr (read-line reader))))
    (display "Subscription: ")
    (display (hash-> ack status))
    (newline)

    (if (equal? (hash-> ack status) "ok")
      (read-events reader 5)
      #f))

  (tcp-shutdown! connection))
