(defpackage :ctelemetry/routes
  (:import-from :ctelemetry/db)
  (:import-from :ctelemetry/web)
  (:import-from :ctelemetry/event)
  (:use :cl :alexandria :iterate))

(in-package :ctelemetry/routes)

(defparameter *default-log-count* 1000)

(ctelemetry/web:define-route sections (:get "^/sections") ()
  (st-json:jso "sections" ctelemetry/event:*sections*))

(defun topic-pattern (arg)
   ;; this handles NIL case, too
  (concatenate 'string arg "%"))

(ctelemetry/web:define-route latest (:get "^/latest(/.*)?") (args)
  (st-json:jso
   "cells"
   (iter (for (topic topic-display-name cell-name cell-display-name count ts value)
          in (ctelemetry/db-commands:get-latest
              :topic-pattern (topic-pattern (first args))))
         (collect
             (list topic
                   (ctelemetry/event:topic-display-name topic topic-display-name)
                   cell-name
                   (ctelemetry/event:cell-display-name topic cell-name cell-display-name)
                   count ts
                   (or (ignore-errors (with-standard-io-syntax
                                        (let ((*read-eval* nil))
                                          (read-from-string value))))
                       value))))))

(ctelemetry/web:define-route log (:get "^/log(/.*)?") (args)
  (let ((topic-pattern (topic-pattern (first args))))
    (st-json:jso
     "topics"
     (ctelemetry/db-commands:get-topics :topic-pattern topic-pattern)
     "events"
     (let ((events (ctelemetry/db-commands:get-events
                    :count *default-log-count*
                    :topic-pattern topic-pattern
                    :topic-ids (when-let ((filter (ctelemetry/web:get-var "filter")))
                                 (iter (for id in (split-sequence:split-sequence #\, filter))
                                       (handler-case
                                           (collect (parse-integer id))
                                         (parse-error ())))))))
       events))))
