(defpackage :ctelemetry/test
  (:import-from :vtf-json)
  (:import-from :ctelemetry/db)
  (:import-from :ctelemetry/routes)
  (:use :cl :alexandria :iterate :vtf))

(in-package :ctelemetry/test)

(define-constant +base-time+ 1420000000)

(defparameter *sample-messages*
  '(("/more-sensors/voltage" "4.5")
    ("/somesensors/temp1" "12")
    ("/somesensors/temp1" "15")
    ("/somesensors/temp2" "42")))

(define-fixture ctelemetry-fixture (vtf-json:abt-json-output-mixin abt-fixture)
  ((rec-items :accessor rec-items)
   (fake-time :accessor fake-time))
  (:default-initargs :data-location '(:asdf ctelemetry #p"abt/")))

(defmethod invoke-test-case-outer ((fixture ctelemetry-fixture) test-case teardown-p)
  (setf (rec-items fixture) (make-array 100 :fill-pointer 0 :adjustable t)
        (fake-time fixture) +base-time+)
  (let ((ctelemetry/db::*db* nil)
        (ctelemetry/event:*sections* '())
        (ctelemetry/event:*cell-overrides*
          (copy-hash-table ctelemetry/event:*cell-overrides*))
        (ctelemetry/event:*topic-overrides*
          (copy-hash-table ctelemetry/event:*topic-overrides*))
        (ctelemetry/event:*event-broadcast-function*
          #'(lambda (json)
              (vector-push-extend json (rec-items fixture))))
        (ctelemetry/event:*current-time-function*
          #'(lambda () (fake-time fixture))))l
    (clrhash ctelemetry/event:*cell-overrides*)
    (clrhash ctelemetry/event:*topic-overrides*)
    (call-next-method)))

(defun configure-for-test ()
  (ctelemetry/event:define-section "/somesensors/" "Some Sensors")
  (ctelemetry/event:define-section "/more-sensors/" "More Sensors")
  (ctelemetry/event:add-subscription-topic "/somesensors/temp1")
  (ctelemetry/event:add-subscription-topic "/somesensors/temp2")
  (ctelemetry/event:add-subscription-topic "/more-sensors/voltage")
  (ctelemetry/event:define-topic "/somesensors/temp1" "Temp One"
    '(("temp1" "Temp")))
  (ctelemetry/event:define-topic "/somesensors/temp2" "Temp Two"
    '(("temp2" "Temp"))))

(defun elapse (duration &optional (fixture *fixture*))
  (incf (fake-time fixture) duration))

(defmethod setup :after ((fixture ctelemetry-fixture))
  (configure-for-test)
  (:printv (hash-table-alist ctelemetry/event:*cell-overrides*))
  (ctelemetry/db:db-setup ":memory:"))

(defmethod teardown :after ((fixture ctelemetry-fixture))
  (ctelemetry/db:db-disconnect))

(deftest test-sections () (ctelemetry-fixture)
  (<< (st-json:jso "url" "GET /sections"
                   "body" (ctelemetry/routes::web-route-sections))))

(defun receive-some-values ()
  (iter (for (topic payload) in *sample-messages*)
        (ctelemetry/event:handle-mqtt-message topic payload)
        (elapse 10)))

(deftest test-latest-values ()  (ctelemetry-fixture)
  (receive-some-values)
  (<< (st-json:jso "url" "GET /latest"
                   "body" (ctelemetry/routes::web-route-latest '())))
  (<< (st-json:jso "url" "GET /latest/somesensors/"
                   "body" (ctelemetry/routes::web-route-latest '("/somesensors")))))

;; TBD: specify overrides for plain values
;; TBD: test multi-value events

;; TBD: actually check method / url in requests, automate request json generation
;; TBD: test receiving proper events
;; TBD: test receiving unparseable values
