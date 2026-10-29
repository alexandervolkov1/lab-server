(require-builtin steel/hash)
(require "../../lib/collections.scm")
(require "../../lib/lab-client.scm")

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

(let ((client (lab-connect "127.0.0.1:7240")))

  (show-response
   (lab-get client 1 "sample_temperature"))

  (show-response
   (lab-get client 2 "heater_temperature"))

  (show-response
   (lab-get client 3 "room_temperature"))

  (show-response
   (lab-get client 4 "power"))

  (lab-close client))
