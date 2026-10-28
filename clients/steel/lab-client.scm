(require-builtin steel/tcp)
(require-builtin steel/json)
(require-builtin steel/hash)

(provide lab-connect lab-close lab-get lab-set lab-subscribe lab-receive)

(define (lab-connect address)
  (let* ((connection (tcp-connect address))
         (writer (tcp-stream-writer connection))
         (reader (tcp-stream-buffered-reader connection)))
    (list connection writer reader)))

(define (lab-close client)
  (tcp-shutdown! (car client)))

(define (lab-request client request)
  (let ((writer (cadr client))
        (reader (caddr client)))
    (display (value->jsexpr-string request) writer)
    (display "\n" writer)

    (string->jsexpr (read-line reader))))

(define (lab-get client id key)
  (lab-request
    client
    (hash "id" id
          "op" "get"
          "key" key)))

(define (lab-set client id key value)
  (lab-request
    client
    (hash "id" id
          "op" "set"
          "key" key
          "value" value)))

(define (lab-subscribe client id key)
  (lab-request
    client
    (hash "id" id
          "op" "subscribe"
          "key" key)))

(define (lab-receive client)
  (string->jsexpr
    (read-line (caddr client))))
