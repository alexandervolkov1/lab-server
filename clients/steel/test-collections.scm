(require-builtin steel/hash)
(require-builtin steel/meta)
(require "collections.scm")

(define data
  (hash 'reading
    (hash 'value
      (hash 'type "float"
        'value
        20.0))))

(assert! (equal? (get-in data '(reading value value)) 20.0))

(assert! (equal? (hash-> data reading value value) 20.0))

(assert! (equal? (get-in data '()) data))

(assert! (equal? (hash-> data) data))

(assert!
  (equal?
    (hash-> data reading value value)
    (get-in data '(reading value value))))

(assert!
  (equal?
    (hash-> data reading value type)
    "float"))

(display "All collections tests passed\n")
