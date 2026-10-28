(require-builtin steel/hash)
(require-builtin steel/time)
(require "lab-client.scm")
(require "collections.scm")
(require "csv.scm")

(provide lab-record-samples)

(define (write-measurement port key response)
  (write-csv-row
    port
    (list
      (hash-ref-or response 'completed_at_unix_ms "")
      key
      (hash-ref-or response 'status "")
      (hash-ref-or response 'value "")
      (hash-ref-or response 'unit "")
      (hash-ref-or response 'error "")))

  (flush-output-port port))

(define (record-once client port keys request-id)
  (if (null? keys)
      request-id

      (let* ((key (car keys))
             (response (lab-get client request-id key)))

        (write-measurement port key response)

        (record-once
          client
          port
          (cdr keys)
          (+ request-id 1)))))

(define (lab-record-samples client keys cycles pause-ms filename)
  (let ((port (open-output-file filename)))

    (write-csv-row
      port
      '("completed_at_unix_ms"
        "key"
        "status"
        "value"
        "unit"
        "error"))

    (flush-output-port port)

    (let loop ((remaining cycles)
               (request-id 1))

      (if (> remaining 0)
          (let ((next-id
                  (record-once client port keys request-id)))

            (if (> remaining 1)
                (time/sleep-ms pause-ms)
                #t)

            (loop (- remaining 1) next-id))

          #t))

    (close-output-port port)))
