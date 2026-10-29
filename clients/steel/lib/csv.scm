(provide write-csv-field write-csv-row)

(define (write-csv-field port value)
  (if (string? value)
      (begin
        (display "\"" port)

        (for-each
         (lambda (character)
           (if (char=? character #\")
               (display "\"\"" port)
               (display character port)))
         (string->list value))

        (display "\"" port))

      (display value port)))

(define (write-csv-row port fields)
  (let loop ((remaining fields))
    (if (null? remaining)
        (newline port)

        (begin
          (write-csv-field port (car remaining))

          (if (null? (cdr remaining))
              #t
              (display "," port))

          (loop (cdr remaining))))))
