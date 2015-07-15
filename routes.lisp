(defpackage :ctelemetry/routes
  (:import-from :ctelemetry/db)
  (:import-from :ctelemetry/web)
  (:import-from :ctelemetry/event)
  (:use :cl :alexandria :iterate))

(in-package :ctelemetry/routes)

(defparameter *default-log-count* 1000)
(define-constant +max-id+ (1- (expt 2 63)))

(ctelemetry/web:define-route sections (:get "^/sections") ()
  (st-json:jso "sections" ctelemetry/event:*sections*))

(defun topic-pattern (arg)
   ;; this handles NIL case, too
  (concatenate 'string arg "%"))

(defun parse-id (id-str)
  (if (> (length id-str) 20)
      nil
      (handler-case
          (let ((n (parse-integer id-str)))
            (when (< 0 n +max-id+) n))
        (parse-error ()))))

(defun parse-ids (id-str)
  (remove nil (mapcar #'parse-id (cl-ppcre:split "[ +]" id-str))))

;; FIXME
(defun parse-event-value (value)
  (or (ignore-errors (with-standard-io-syntax
                       (let ((*read-eval* nil))
                         (read-from-string value))))
      value))

(ctelemetry/web:define-route latest (:get "^/latest(/.*)?") (args)
  (let* ((topic-index (make-hash-table :test #'equal))
         (cell-index (make-hash-table :test #'equal))
         (cell-data
           (iter (for (topic topic-display-name cell-id cell-name cell-display-name count ts value)
                  in (ctelemetry/db-commands:get-latest
                      :topic-pattern (topic-pattern (first args))))
                 (setf (gethash topic topic-index)
                       (list (ctelemetry/event:topic-display-name topic topic-display-name)
                             count
                             ts)
                       (gethash (cons topic cell-name) cell-index)
                       cell-id)
                 (collect
                     (list topic
                           (ctelemetry/event:topic-display-name topic topic-display-name)
                           cell-id
                           cell-name
                           (ctelemetry/event:cell-display-name topic cell-name cell-display-name)
                           count ts
                           (parse-event-value value)))))
         (group-data
           (iter (for group in (sort (hash-table-values ctelemetry/event:*groups*)
                                #'string<
                                :key #'ctelemetry/event:value-group-title))
                 (let* ((topic (ctelemetry/event:value-group-topic group))
                        (topic-info (gethash topic topic-index))
                        (cells (iter (for cell in (ctelemetry/event:value-group-cells group))
                                     (when-let ((cell-id (gethash (cons topic cell) cell-index)))
                                       (collect cell-id)))))
                   (when (and topic-info cells)
                     (destructuring-bind (topic-display-name count ts) topic-info
                       (collect
                           (list topic
                                 topic-display-name
                                 (format nil "~{~a~^+~}" cells)
                                 (format nil "~{~a~^,~}" (ctelemetry/event:value-group-cells group))
                                 (ctelemetry/event:value-group-title group)
                                 count ts
                                 :null))))))))
    (st-json:jso
     "cells" (append group-data cell-data))))

(ctelemetry/web:define-route log (:get "^/log(/.*)?") (args)
  (let ((topic-pattern (topic-pattern (first args))))
    (st-json:jso
     "topics"
     (iter (for (id topic display-name) in
            (ctelemetry/db-commands:get-topics :topic-pattern topic-pattern))
           (collect (list id topic
                          (ctelemetry/event:topic-display-name topic display-name))))
     "events"
     (let ((filter (ctelemetry/web:get-var "filter")))
       ;; empty string as a filter means 'nothing'
       (when (or (null filter) (not (string= "" filter)))
         (ctelemetry/db-commands:get-events
          :count *default-log-count*
          :topic-pattern topic-pattern
          :topic-ids (when filter
                       (remove nil (mapcar #'parse-id
                                           (split-sequence:split-sequence #\, filter))))
          :start (or (when-let ((start (ctelemetry/web:get-var "start")))
                       (parse-integer start :junk-allowed t))
                     0)))))))

;; \d{1,20} due to 64-bit ids
(ctelemetry/web:define-route event (:get "^/event/(\\d{1,20})") (args)
  (when-let ((id (parse-id (first args))))
    (let (topic topic-display-name timestamp)
      (st-json:jso
       "cells"
       (iter (for (r-topic r-topic-display-name cell-name cell-display-name r-timestamp value)
              in (ctelemetry/db-commands:load-event :id id))
             (if-first-time
              (setf topic r-topic
                    topic-display-name (ctelemetry/event:topic-display-name
                                        r-topic r-topic-display-name)
                    timestamp r-timestamp))
             (when cell-name
               (collect
                   (st-json:jso "cellName"
                                cell-name
                                "cellDisplayName"
                                (ctelemetry/event:cell-display-name topic cell-name cell-display-name)
                                "value" (parse-event-value value)))))
       "topic" topic
       "topicDisplayName" topic-display-name
       "timestamp" timestamp))))

(ctelemetry/web:define-route history (:get "^/history/(\\d{1,20}(?:[+ ]\\d{1,20})*)") (args)
  (when-let ((ids (parse-ids (first args))))
    (st-json:jso
     "history"
     (iter (for (event-id timestamp . values)
            in (ctelemetry/db-commands:get-history :cell-ids ids))
           (collect (list* event-id timestamp (mapcar #'parse-event-value values)))))))
