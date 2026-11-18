(require-builtin steel/tcp)
(require-builtin steel/json)
(require-builtin steel/hash)
(require "collections.scm")

(provide lab-connect lab-close lab-get lab-set lab-subscribe lab-receive)

(define event-limit 256)

; Incoming events go onto rear. Reverse it only when front is empty.
(struct lab-client (connection writer reader front rear event-count) #:mutable)

(define (lab-connect address)
  (let* ((connection (tcp-connect address))
         (writer (tcp-stream-writer connection))
         (reader (tcp-stream-buffered-reader connection)))
    (lab-client connection writer reader '() '() 0)))

(define (lab-close client)
  (let ((connection (lab-client-connection client)))
    (when connection
      (set-lab-client-connection! client #f)
      (with-handler (lambda (_) #f) (tcp-shutdown! connection))
      (close-port (lab-client-writer client))
      (close-port (lab-client-reader client)))))

(define (require-connection client)
  (unless (lab-client-connection client)
    (error "lab-client: connection is closed; reconnect")))

(define (fail-connection client message)
  (lab-close client)
  (error (string-append "lab-client: " message "; reconnect")))

(define (measurement? message)
  (and (not (hash-contains? message 'id))
       (equal? (hash-ref-or message 'event #f) "measurement")))

(define (read-message client)
  (require-connection client)
  (let ((line (with-handler (lambda (err) err)
                (read-line-from-port (lab-client-reader client)))))
    (when (error-object? line)
      (fail-connection client (string-append "TCP read failed: " (error-object-message line))))
    (when (eof-object? line)
      (fail-connection client "unexpected EOF"))
    (unless (ends-with? line "\n")
      (fail-connection client "EOF before the end of a JSON frame"))
    (let ((message (with-handler (lambda (err) err) (string->jsexpr line))))
      (when (error-object? message)
        (fail-connection client "invalid JSON frame"))
      (unless (hash? message)
        (fail-connection client "expected a JSON object"))
      (when (and (not (hash-contains? message 'id))
                 (not (measurement? message))
                 (equal? (hash-ref-or message 'status #f) "error"))
        (fail-connection client
                         (string-append "server error without id: "
                                        (to-string (hash-ref-or message 'error "unknown error")))))
      (cons message line))))

; The JSON decoder converts numbers to f64. Extract the top-level id token
; from the already validated JSON to compare the full unsigned integer exactly.
(define (json-space chars)
  (if (and (pair? chars) (char-whitespace? (car chars)))
      (json-space (cdr chars))
      chars))

(define (json-string-token chars)
  (let loop ((rest (cdr chars)) (rev '(#\")))
    (case (car rest)
      ((#\\) (loop (cddr rest) (cons (cadr rest) (cons #\\ rev))))
      ((#\") (cons (list->string (reverse (cons #\" rev))) (cdr rest)))
      (else (loop (cdr rest) (cons (car rest) rev))))))

(define (wire-id line)
  (let scan ((chars (string->list line)) (depth 0) (id #f))
    (if (null? chars)
        id
        (case (car chars)
          ((#\{ #\[) (scan (cdr chars) (+ depth 1) id))
          ((#\} #\]) (scan (cdr chars) (- depth 1) id))
          ((#\")
           (let* ((token (json-string-token chars))
                  (rest (json-space (cdr token))))
             (if (and (= depth 1) (pair? rest) (char=? (car rest) #\:)
                      (equal? (string->jsexpr (car token)) "id"))
                 (if id
                     #f ; Duplicate id fields are ambiguous.
                     (let digits ((rest (json-space (cdr rest))) (rev '()))
                       (if (and (pair? rest) (char<=? #\0 (car rest) #\9))
                           (digits (cdr rest) (cons (car rest) rev))
                           (let ((delimiter (json-space rest)))
                             (if (and (pair? rev) (pair? delimiter)
                                      (memv (car delimiter) '(#\, #\})))
                                 (scan delimiter depth (list->string (reverse rev)))
                                 #f)))))
                 (scan rest depth id))))
          (else (scan (cdr chars) depth id))))))

(define (enqueue-measurement client message)
  (when (= (lab-client-event-count client) event-limit)
    (fail-connection client "measurement queue overflow"))
  (set-lab-client-rear! client (cons message (lab-client-rear client)))
  (set-lab-client-event-count! client (+ (lab-client-event-count client) 1)))

(define (lab-request client id request)
  (require-connection client)
  (unless (and (integer? id) (exact? id) (<= 1 id) (<= id 18446744073709551615))
    (error "lab-client: request id must be a positive u64"))
  (let* ((expected-id (number->string id))
         (body (value->jsexpr-string request))
         (writer (lab-client-writer client))
         ; Encode id separately so it remains exact even above Steel's native integer range.
         (frame (string-append "{\"id\":" expected-id "," (substring body 1) "\n"))
         (failure (with-handler (lambda (err) err)
                    (display frame writer)
                    (flush-output-port writer)
                    #f)))
    (when failure
      (fail-connection client (string-append "TCP write failed: " (error-object-message failure))))
    (let wait-for-reply ()
      (let* ((frame (read-message client))
             (message (car frame)))
        (cond
          ((measurement? message)
           (enqueue-measurement client message)
           (wait-for-reply))
          ((hash-contains? message 'id)
           (if (equal? (wire-id (cdr frame)) expected-id)
               (hash-insert message 'id id)
               (fail-connection client "unexpected response id")))
          (else (fail-connection client "unexpected message while waiting for a response")))))))

(define (lab-get client id key)
  (lab-request
   client id
   (hash "op" "get"
         "key" key)))

(define (lab-set client id key value)
  (lab-request
   client id
   (hash "op" "set"
         "key" key
         "value" value)))

(define (lab-subscribe client id key)
  (lab-request
   client id
   (hash "op" "subscribe"
         "key" key)))

(define (lab-receive client)
  (if (> (lab-client-event-count client) 0)
      (begin
        (when (null? (lab-client-front client))
          (set-lab-client-front! client (reverse (lab-client-rear client)))
          (set-lab-client-rear! client '()))
        (let ((event (car (lab-client-front client))))
          (set-lab-client-front! client (cdr (lab-client-front client)))
          (set-lab-client-event-count! client (- (lab-client-event-count client) 1))
          event))
      (let ((message (car (read-message client))))
        (if (measurement? message)
            message
            (fail-connection client "expected a measurement without id")))))
