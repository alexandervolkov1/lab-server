(require "collections.scm")
(require "lab-client.scm")

(define (show-measurement event)
  (display (hash-> event key))
  (display ": ")

  (cond
    ((equal? (hash-> event status) "ok")
      (display (hash-> event value))
      (display " ")
      (display (hash-> event unit)))

    ((equal? (hash-> event status) "error")
      (display "ERROR: ")
      (display (hash-> event error)))

    (else
      (display "Unexpected status: ")
      (display (hash-> event status))))

  (display " at ")
  (display (hash-> event completed_at_unix_ms))
  (newline))

(define (read-events client remaining)
  (if (> remaining 0)
      (begin
        (show-measurement (lab-receive client))
        (read-events client (- remaining 1)))
      #t))

(let ((client (lab-connect "127.0.0.1:7240")))

  (let ((response
          (lab-subscribe client 1 "sample_temperature")))

    (display "Subscription: ")
    (display (hash-> response status))
    (newline)

    (if (equal? (hash-> response status) "ok")
        (read-events client 5)
        #f))

  (lab-close client))
