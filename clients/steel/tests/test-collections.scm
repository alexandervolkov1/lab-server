(require-builtin steel/hash)
(require-builtin steel/meta)
(require "../lib/collections.scm")

(define data
  (hash 'reading
    (hash 'value
      (hash 'type "float"
        'value
        20.0))))

(assert! (equal? (hash-ref-in data '(reading value value)) 20.0))

(assert! (equal? (hash-> data reading value value) 20.0))

(assert! (equal? (hash-ref-in data '()) data))

(assert! (equal? (hash-> data) data))

(assert!
  (equal?
    (hash-> data reading value value)
    (hash-ref-in data '(reading value value))))

(assert!
  (equal?
    (hash-> data reading value type)
    "float"))

(assert!
  (equal?
    (hash-ref-or data 'reading "missing")
    (hash-ref data 'reading)))

(assert!
  (equal?
    (hash-ref-or data 'unknown "missing")
    "missing"))

(assert!
  (equal?
    (hash-ref-or (hash 'enabled #f) 'enabled #t)
    #f))

(display "All collections tests passed\n")
