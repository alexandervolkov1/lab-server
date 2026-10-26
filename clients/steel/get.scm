(require-builtin steel/tcp)
(require-builtin steel/time)
(require-builtin steel/json)
(require-builtin steel/hash)
(require "collections.scm")

(define (get-latest writer reader id)
  (display "{\"id\":" writer)
  (display id writer)
  (display ",\"op\":\"get\",\"key\":\"sample_temperature\"}\n" writer)
  (flush-output-port writer)
  (string->jsexpr (read-line reader)))

(define (show-response response)
  (let ((status (hash-> response status)))
    (cond
      ((equal? status "ok")
        (display (hash-> response key))
        (display ": ")
        (display (hash-> response value))
        (display " ")
        (display (hash-> response unit))
        (newline))

      ((equal? status "error")
        (display "Server error: ")
        (display (hash-> response error))
        (newline))

      (else
        (display "Server status: ")
        (display status)
        (newline)))))

(define (poll writer reader id remaining)
  (if (= remaining 0)
    #f
    (begin
      (show-response (get-latest writer reader id))
      (time/sleep-ms 1000)
      (poll writer reader (+ id 1) (- remaining 1)))))

(let* ((connection (tcp-connect "127.0.0.1:7240"))
       (writer (tcp-stream-writer connection))
       (reader (tcp-stream-buffered-reader connection)))
  (poll writer reader 1 5)
  (tcp-shutdown! connection))
