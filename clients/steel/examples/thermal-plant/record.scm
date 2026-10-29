(require-builtin steel/time)

(require "../../lib/lab-client.scm")
(require "../../lib/lab-record.scm")

(define output-file
  (string-append
   "clients/steel/output/thermal-plant-"
   (number->string (current-milliseconds))
   ".csv"))

(define parameters
  '("sample_temperature"
    "heater_temperature"
    "room_temperature"
    "power"))

(let ((client (lab-connect "127.0.0.1:7240")))

  (lab-record-samples
   client
   parameters
   10
   1000
   output-file)

  (lab-close client))

(display "Measurements saved to: ")
(display output-file)
(newline)
