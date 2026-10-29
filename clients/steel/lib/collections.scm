(require-builtin steel/hash)

(provide hash-ref-in hash-ref-or hash->)

(define (hash-ref-in data path)
  (let loop ((current data)
             (keys path))
    (if (null? keys)
        current
        (loop (hash-ref current (car keys))
              (cdr keys)))))

(define (hash-ref-or data key default)
  (if (hash-contains? data key)
      (hash-ref data key)
      default))

(define-syntax hash->
  (syntax-rules ()
    ((_ data)
     data)
    ((_ data key rest ...)
     (hash-> (hash-ref data 'key) rest ...))))
