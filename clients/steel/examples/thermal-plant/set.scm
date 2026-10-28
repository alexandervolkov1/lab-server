(require-builtin steel/time)
(require-builtin steel/hash)
(require "../../lib/collections.scm")
(require "../../lib/lab-client.scm")

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

(let ((client (lab-connect "127.0.0.1:7240")))

  (show-response 0
    (lab-set client 1 "power" 0))

  (show-response 10
    (lab-set client 2 "power" 10))

  (time/sleep-ms 10000)

  (show-response 0
    (lab-set client 3 "power" 0))

  (lab-close client))
