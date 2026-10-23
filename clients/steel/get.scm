(require-builtin steel/tcp)
(require-builtin steel/time)
(require-builtin steel/json)
(require-builtin steel/hash)
(require "collections.scm")

(define (get-latest writer reader)
  (display "GET\n" writer)
  (flush-output-port writer)
  (string->jsexpr (read-line reader)))

(define (show-response response)
  (if (hash-contains? response 'reading)
    (let ((reading (hash-> response reading)))
      (if (equal? (hash-> reading status) "ok")
        (let ((temperature (hash-> reading value value)))
          (display "Temperature: ")
          (display temperature)
          (display " degC")
          (newline))
        (begin
          (display "Measurement error: ")
          (display (hash-> reading message))
          (newline))))
    (begin
      (display "Server status: ")
      (display (hash-> response status))
      (newline))))

(define (poll writer reader remaining)
  (if (= remaining 0)
    #f
    (begin
      (show-response (get-latest writer reader))
      (time/sleep-ms 1000)
      (poll writer reader (- remaining 1)))))

(let* ((connection (tcp-connect "127.0.0.1:7240"))
       (writer (tcp-stream-writer connection))
       (reader (tcp-stream-buffered-reader connection)))
  (poll writer reader 5)
  (tcp-shutdown! connection))
