(require-builtin steel/tcp)
(require-builtin steel/time)

(define (get-latest address)
  (let* ((connection (tcp-connect address))
         (writer (tcp-stream-writer connection))
         (reader (tcp-stream-buffered-reader connection)))

    (display "GET\n" writer)
    (flush-output-port writer)

    (let ((response (read-line reader)))
      (tcp-shutdown! connection)
      response)))

(define (poll address remaining)
  (if (= remaining 0)
    #f
    (begin
      (display (get-latest address))
      (newline)
      (time/sleep-ms 1000)
      (poll address (- remaining 1)))))

(poll "127.0.0.1:7240" 5)
