(require "../lib/csv.scm")

(define (assert-equal expected actual)
  (if (equal? expected actual)
      #t
      (error "Expected:" expected "Actual:" actual)))

(define (field->string value)
  (let ((port (open-output-string)))
    (write-csv-field port value)
    (get-output-string port)))

(define (row->string fields)
  (let ((port (open-output-string)))
    (write-csv-row port fields)
    (get-output-string port)))

(assert-equal
  "\"temperature\""
  (field->string "temperature"))

(assert-equal
  "20.5"
  (field->string 20.5))

(assert-equal
  "\"heater, sample\""
  (field->string "heater, sample"))

(assert-equal
  "\"He said \"\"Hello\"\"\""
  (field->string "He said \"Hello\""))

(assert-equal
  "\"\""
  (field->string ""))

(assert-equal
  "\"line 1\nline 2\""
  (field->string "line 1\nline 2"))

(assert-equal
  "\"temperature\",20.5,\"degC\"\n"
  (row->string '("temperature" 20.5 "degC")))

(display "CSV tests passed")
(newline)
