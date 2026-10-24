(require-builtin steel/hash)

(provide get-in hash->)

(define (get-in data path)
  (let loop ((current data)
             (keys path))
    (if (null? keys)
        current
        (loop (hash-ref current (car keys))
              (cdr keys)))))

(define-syntax hash->
  (syntax-rules ()
    ((_ data)
      data)
    ((_ data key rest ...)
     (hash-> (hash-ref data 'key) rest ...))))
