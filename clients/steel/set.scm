(require-builtin steel/tcp)
(require-builtin steel/json)
(require-builtin steel/time)
(require-builtin steel/hash)
(require "collections.scm")

(define (set-power writer reader id power)
  (display "{\"id\":" writer)
  (display id writer)
  (display ",\"op\":\"set\",\"key\":\"power\",\"value\":" writer)
  (display power writer)
  (display "}\n" writer)
  (flush-output-port writer)
  (string->jsexpr (read-line reader)))

(define (show-response power response)
  (display "Set power to ")
  (display power)
  (display "%: ")
  (display (hash-> response status))

  (if (equal? (hash-> response status) "error")
    (begin
      (display " - ")
      (display (hash-> response error)))
    #f)

  (newline))

(let* ((connection (tcp-connect "127.0.0.1:7240"))
       (writer (tcp-stream-writer connection))
       (reader (tcp-stream-buffered-reader connection)))

  (show-response 0 (set-power writer reader 1 0))

  (show-response 10 (set-power writer reader 2 10))

  (time/sleep-ms 10000)

  (show-response 0 (set-power writer reader 3 0))

  (tcp-shutdown! connection))
