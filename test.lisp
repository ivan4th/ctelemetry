(defpackage :ctelemetry/test
  (:import-from :vtf-json)
  (:import-from :ctelemetry/db)
  (:import-from :ctelemetry/routes)
  (:use :cl :alexandria :iterate :vtf))

(in-package :ctelemetry/test)

(define-constant +base-time+ 1420000000)

(define-fixture ctelemetry-fixture (vtf-json:abt-json-output-mixin abt-fixture)
  ((fake-time :accessor fake-time))
  (:default-initargs :data-location '(:asdf ctelemetry #p"abt/")))

(defparameter *sample-messages*
  (list '("/more-sensors/voltage" "4.5")
        '("/somesensors/temp1" "12")
        '("/somesensors/temp1" "15")
        '("/somesensors/temp2" "42")
        (list "/more-sensors/events/whatever"
              #'(lambda (fixture)
                  (prin1-to-string
                   (list "Whatever"
                         (fake-time fixture)
                         '(:cell1 42d0 "Cell One")
                         '(:cell2 42.42d0 "Cell Two")))))))


(defmethod invoke-test-case-outer ((fixture ctelemetry-fixture) test-case teardown-p)
  (setf (fake-time fixture) +base-time+)
  (let ((ctelemetry/db::*db* nil)
        (ctelemetry/event:*sections* '())
        (ctelemetry/event:*cell-overrides*
          (copy-hash-table ctelemetry/event:*cell-overrides*))
        (ctelemetry/event:*topic-overrides*
          (copy-hash-table ctelemetry/event:*topic-overrides*))
        (ctelemetry/event:*event-broadcast-function* #'<<)
        (ctelemetry/event:*current-time-function*
          #'(lambda () (fake-time fixture))))
    (clrhash ctelemetry/event:*cell-overrides*)
    (clrhash ctelemetry/event:*topic-overrides*)
    (call-next-method)))

(defun configure-for-test ()
  (ctelemetry/event:define-section "/somesensors/" "Some Sensors")
  (ctelemetry/event:define-section "/more-sensors/" "More Sensors")
  (ctelemetry/event:add-subscription-topic "/somesensors/temp1")
  (ctelemetry/event:add-subscription-topic "/somesensors/temp2")
  (ctelemetry/event:add-subscription-topic "/more-sensors/voltage")
  (ctelemetry/event:add-subscription-topic "/+/events/#")
  (ctelemetry/event:define-topic "/somesensors/temp1" "Temp One"
    '(("temp1" "Temp")))
  (ctelemetry/event:define-topic "/somesensors/temp2" "Temp Two"
    '(("temp2" "Temp"))))

(defun elapse (duration &optional (fixture *fixture*))
  (incf (fake-time fixture) duration))

(defmethod setup :after ((fixture ctelemetry-fixture))
  (configure-for-test)
  (ctelemetry/db:db-setup ":memory:"))

(defmethod teardown :after ((fixture ctelemetry-fixture))
  (ctelemetry/db:db-disconnect))

(defun invoke-request (method url)
  (<< (st-json:jso "request" (format nil "~a ~a" method url)
                   "body" (ctelemetry/web:invoke-route method url))))

(deftest test-sections () (ctelemetry-fixture)
  (invoke-request :get "/sections"))

(defun receive-some-values ()
  (iter (for (topic payload) in *sample-messages*)
        (ctelemetry/event:handle-mqtt-message
         topic (if (functionp payload)
                   (funcall payload *fixture*)
                   payload))
        (elapse 10)))

(deftest test-latest-values ()  (ctelemetry-fixture)
  (receive-some-values)
  (invoke-request :get "/latest")
  (invoke-request :get "/latest/somesensors/"))

(deftest test-log () (ctelemetry-fixture)
  (receive-some-values)
  (invoke-request :get "/log")
  (invoke-request :get "/log?filter=2,3")
  (invoke-request :get "/log/more-sensors/")
  (invoke-request :get "/log/more-sensors/?filter=4")
  ;; non-realistic query (topic 2 is not under /more-sensors)
  (invoke-request :get "/log/more-sensors/?filter=2,4")
  (invoke-request :get "/log?filter=2,3&start=1420000020")
  (invoke-request :get "/log?start=1420000020"))

;; TBD: specify overrides for plain values
;; TBD: test multi-value events

;; TBD: actually check method / url in requests, automate request json generation
;; TBD: test receiving proper events
;; TBD: test receiving unparseable values
