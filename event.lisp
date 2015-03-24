(defpackage :ctelemetry/event
  (:import-from :i4-diet-utils) ;; FIXME
  (:import-from :ctelemetry/db)
  (:import-from :ctelemetry/db-commands)
  (:import-from :ctelemetry/util)
  (:import-from :parse-number)
  (:use :cl :alexandria :iterate)
  (:export #:*topic-overrides*
           #:*cell-overrides*
           #:*sections*
           #:*subscribe-topics*
           #:*event-broadcast-function*
           #:*current-time-function*
           #:add-subscription-topic
           #:define-topic
           #:define-section
           #:handle-mqtt-message
           #:topic-display-name
           #:cell-display-name))

(in-package :ctelemetry/event)

(defparameter *max-payload-size* 8192)
(defvar *topic-overrides* (make-hash-table :test #'equal))
(defvar *cell-overrides* (make-hash-table :test #'equal))
(defvar *sections* '())
(defvar *subscribe-topics* '())
(defvar *event-broadcast-function*
  #'(lambda (event)
      (declare (ignore event))
      (values)))
(defvar *current-time-function*
  #'(lambda ()
      (coerce (ctelemetry/util:current-time) 'double-float)))

(define-condition telemetry-error (simple-error) ())

(defun telemetry-error (fmt &rest args)
  (error 'telemetry-error :format-control fmt :format-arguments args))

(defclass telemetry-event ()
  ((topic        :accessor topic         :initarg :topic)
   (display-name :accessor display-name  :initarg :display-name)
   (timestamp    :accessor timestamp     :initarg :timestamp)
   (cell-values  :accessor cell-values   :initarg :cell-values)))

(defun store-telemetry-event (event)
  (ctelemetry/db:with-db-transaction
    (let* ((topic-id (ctelemetry/db-commands:ensure-topic
                      :topic (topic event)
                      :display-name (display-name event)))
           (event-id (ctelemetry/db-commands:store-event
                      :timestamp (timestamp event)
                      :topic-id topic-id)))
      (iter (for (cell-name cell-value cell-display-name) in (cell-values event))
            (let ((cell-id (ctelemetry/db-commands:ensure-topic-cell
                            :topic-id topic-id
                            :name (string-downcase cell-name)
                            :display-name cell-display-name))
                  (value-str (with-standard-io-syntax
                               (prin1-to-string cell-value))))
              (ctelemetry/db-commands:store-event-value
               :event-id event-id
               :cell-id cell-id
               :value value-str)
              (ctelemetry/db-commands:update-cell
               :timestamp (timestamp event)
               :value value-str
               :topic-id topic-id
               :cell-id cell-id))))))

#++
(defun sample-telemetry-event (&optional (topic "/some/topic") (display-name "Something Happened"))
  (make-instance 'telemetry-event
                 :topic topic
                 :display-name display-name
                 :timestamp (funcall *current-time-function*)
                 :cell-values
                 '((:some-cell 42d0 "Some Cell")
                   (:another-cell 54 "Another Cell")
                   (:str-cell "zzz" "String Cell"))))

(defun topic-cell-name (topic)
  (or (i4-diet-utils:with-match (cell-name) ("^.*?([^/]+)$" topic)
        cell-name)
      (telemetry-error "cannot get cell name from topic ~s" topic)))

(defun make-simple-telemetry-event (topic value)
  (let ((cell-name (topic-cell-name topic)))
    (make-instance 'telemetry-event
                   :topic topic
                   :display-name cell-name
                   :timestamp (funcall *current-time-function*)
                   :cell-values (list
                                 (list (make-keyword (string-upcase cell-name))
                                       value
                                       cell-name)))))

(defun make-complex-telemetry-event (topic parsed-payload)
  (unless (typep parsed-payload
                 '(cons string (cons (real 0) proper-list)))
    (telemetry-error "invalid complex event: ~s" parsed-payload))
  (destructuring-bind (display-name timestamp &rest cell-values)
      parsed-payload
    (dolist (item cell-values)
      (unless (typep item '(cons keyword (cons t (cons string null))))
        (telemetry-error "invalid cell value ~s in complex event ~s"
                         item parsed-payload)))
    (make-instance 'telemetry-event
                   :topic topic
                   :display-name display-name
                   :timestamp timestamp
                   :cell-values cell-values)))

(defun parse-event (topic event-str)
  (when (> (length event-str) *max-payload-size*)
    (telemetry-error "payload too large for topic ~s" topic))
  (let ((parsed-payload (handler-case
                            (with-standard-io-syntax
                              (let ((*read-eval* nil)
                                    (*package* (find-package :ctelemetry/main)))
                                (read-from-string event-str)))
                          (end-of-file ()
                            (telemetry-error "reader: eof"))
                          (reader-error (c)
                            (telemetry-error "reader error: ~a" c)))))
    (if (atom parsed-payload)
        (make-simple-telemetry-event topic parsed-payload)
        (make-complex-telemetry-event topic parsed-payload))))

(defun handle-mqtt-message (topic payload)
  (handler-case
      (parse-event topic payload)
    (telemetry-error (c)
      (warn "failed to parse event: ~a" c))
    (:no-error (event)
      (store-telemetry-event event)
      (funcall *event-broadcast-function*
               (st-json:jso
                "topic" (topic event)
                "displayName" (display-name event)
                "ts" (timestamp event)
                "cells" (iter (for (cell-name cell-value nil) in (cell-values event))
                              (collect (list (string-downcase cell-name) cell-value))))))))

(defun add-subscription-topic (topic)
  (pushnew topic *subscribe-topics* :test #'equal))

(defun define-topic (topic topic-display-name cells)
  (when topic-display-name
    (setf (gethash topic *topic-overrides*) topic-display-name))
  (iter (for (cell-name cell-display-name) in cells)
        (setf (gethash (cons topic cell-name) *cell-overrides*)
              cell-display-name)))

(defun define-section (topic-prefix title)
  (setf *sections*
        (sort (adjoin (list topic-prefix title) *sections* :test #'equal)
              #'string<
              :key #'first)))

(defun topic-display-name (topic &optional default)
  (or (gethash topic *topic-overrides*) default))

(defun cell-display-name (topic cell-name &optional default)
  (or (gethash (cons topic cell-name) *cell-overrides*) default))
