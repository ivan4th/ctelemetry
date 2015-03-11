(defpackage :ctelemetry/main
  (:import-from :blackbird)
  (:import-from :cl-async)
  (:import-from :cl-async-repl)
  (:import-from :sqlite)
  (:import-from :printv)
  (:import-from :log4cl)
  #++
  (:import-from :osicat) ;; needs an .so
  (:import-from :babel)
  (:import-from :cl-mqtt)
  (:import-from :i4-diet-utils)
  (:import-from :cl-async-repl)
  (:import-from :wookie)
  (:import-from :st-json)
  (:import-from :uiop)
  (:import-from :swank) ;; FIXME: doesn't seem to work?
  (:import-from :websocket-driver)
  (:use :cl :alexandria :iterate))

(in-package :ctelemetry/main)

(defparameter *config-file-name* #p".ctelemetryrc")

;; TBD: cell types
;; TBD: schema versioning
(defparameter *table-ddl*
  '("create table if not exists topics (
        id integer not null primary key autoincrement,
        topic text not null,
        display_name text)"
    "create unique index if not exists topics_topic on topics (topic)"
    "create table if not exists topic_cells (
        id integer not null primary key autoincrement,
        topic_id integer not null references topics(id),
        name text,
        display_name text)"
    "create unique index if not exists topic_cells_topic_id_name
        on topic_cells (topic_id, name)"
    "create table if not exists events (
        id integer not null primary key autoincrement,
        timestamp real,
        topic_id integer not null references topics(id))"
    "create index if not exists events_timestamp on events (timestamp)"
    "create table if not exists event_values (
        event_id integer not null references events(id),
        cell_id integer not null references topic_cells(id),
        value text,
        primary key(event_id, cell_id))"
    "create index if not exists event_values_cell_id on event_values(cell_id)"))

(defparameter *sql-commands*
  '((ensure-topic
     "insert or replace into topics (id, topic, display_name)
      values ((select id from topics where topic = :topic), :topic, :display_name)")
    (ensure-topic-cell
     "insert or replace into topic_cells (id, topic_id, name, display_name)
      values ((select id from topic_cells where topic_id = :topic_id and name = :name),
              :topic_id, :name, :display_name)")
    (store-event
     "insert into events (timestamp, topic_id) values (:timestamp, :topic_id)")
    (store-event-value
     "insert into event_values (event_id, cell_id, value)
      values (:event_id, :cell_id, :value)")
    (get-events
     "select t.topic, t.display_name, e.id, e.timestamp, e.topic_id
      from events e join topics t on e.topic_id = t.id
      order by e.id desc limit 100")
    (get-latest
     ;; FIXME: slow & stupid
     "select t.topic, t.display_name as topic_display_name,
             tc.name as cell_name, tc.display_name as cell_display_name,
             (select count(*)
                     from event_values ev
                     join events e on ev.event_id = e.id
                     where ev.cell_id = tc.id) as count,
             (select e.timestamp
                     from event_values ev
                     join events e on ev.event_id = e.id
                     where ev.cell_id = tc.id
                     order by e.id desc limit 1) as ts,
             (select ev.value
                     from event_values ev
                     join events e on ev.event_id = e.id
                     where ev.cell_id = tc.id
                     order by e.id desc limit 1) as value
      from topics t
           join topic_cells tc on tc.topic_id = t.id
      where t.topic like :topic_pattern
      order by topic_display_name, cell_name")))

(defparameter *max-payload-size* 8192)
(defparameter *db-file* #p"/var/lib/ctelemetry/telemetry.db")
(defparameter *mqtt-host* "localhost")
(defparameter *mqtt-port* 1883)
(defparameter *www-port* 8999)
(defvar *db* nil)
(defvar *mqtt* nil)
(defvar *topic-overrides* (make-hash-table :test #'equal))
(defvar *cell-overrides* (make-hash-table :test #'equal))
(defvar *subscribe-topics* '())
(defvar *sections* '())

(defun bind-params (params)
  (iter (for (name value) on params by #'cddr)
        (collect (if (stringp name)
                     name
                     (concatenate 'string ":"
                                  (substitute #\_ #\- (string-downcase name)))))
        (collect value)))

(defun query-string (query)
  (cond ((stringp query) query)
        ((second (assoc query *sql-commands*)))
        (t
         (error "bad query spec: ~s" query))))

(macrolet ((def-exec-query (name sqlite-name)
             `(defun ,name (query &rest params)
                (apply #',sqlite-name
                       *db*
                       (query-string query)
                       (bind-params params)))))
  (def-exec-query execute-non-query sqlite:execute-non-query/named)
  (def-exec-query execute-to-list sqlite:execute-to-list/named)
  (def-exec-query execute-single sqlite:execute-single/named)
  (def-exec-query execute-one-row-m-v sqlite:execute-one-row-m-v/named))

(defun ensure-schema ()
  (sqlite:with-transaction *db*
    (cond #++((execute-single csdb 'locate-events-table)
              (%csdb-maybe-upgrade csdb))
	  (t
	   (log4cl:log-info "creating db schema from scratch")
	   (dolist (ddl-command *table-ddl*)
             (:printv ddl-command)
	     (execute-non-query ddl-command))
           #++
	   (%csdb-set-version csdb (csdb-required-version))))))

(defun db-setup (&optional (db-file ":memory:"))
  (format t "~%DB: ~s~%" *db*)
  (unless *db*
    (setf *db* (sqlite:connect db-file))
    (format t "~%DB CONN: ~s~%" *db*)
    (ensure-schema)))

(defun ppr (value &optional title)
  (bb:attach-errback
   (bb:attach value
              #'(lambda (&rest values)
                  (format t "~&*** PROMISE~@[ ~a~] RESULT: ~{~s~^ ~}"
                          title values)))
   #'(lambda (e)
       (format t "~&*** PROMISE~@[ ~a~] ERROR: ~a: ~a"
               title (type-of e) e))))

(define-condition telemetry-error (simple-error) ())

(defun telemetry-error (fmt &rest args)
  (error 'telemetry-error :format-control fmt :format-arguments args))

(defclass telemetry-event ()
  ((topic        :accessor topic         :initarg :topic)
   (display-name :accessor display-name  :initarg :display-name)
   (timestamp    :accessor timestamp     :initarg :timestamp)
   (cell-values  :accessor cell-values   :initarg :cell-values)))

(defun exec-and-get-rowid (query &rest params)
  (apply #'execute-non-query query params)
  (sqlite:last-insert-rowid *db*))

(defun store-telemetry-event (event)
  (sqlite:with-transaction *db*
    (let* ((topic-id (exec-and-get-rowid
                      'ensure-topic
                      :topic (topic event)
                      :display-name (display-name event)))
           (event-id (exec-and-get-rowid
                      'store-event
                      :timestamp (timestamp event)
                      :topic-id topic-id)))
      (iter (for (cell-name cell-value cell-display-name) in (cell-values event))
            (let ((cell-id (exec-and-get-rowid
                            'ensure-topic-cell
                            :topic-id topic-id
                            :name (string-downcase cell-name)
                            :display-name cell-display-name)))
              (execute-non-query 'store-event-value
                                 :event-id event-id
                                 :cell-id cell-id
                                 :value (with-standard-io-syntax
                                          (prin1-to-string cell-value))))))))

(defun current-time ()
  (coerce (i4-diet-utils:universal-time->unix-timestamp (get-universal-time)) 'double-float)
  #++
  (multiple-value-bind (sec usec)
      (osicat-posix:gettimeofday)
    (+ sec (/ usec 1d6))))

#++
(defun sample-telemetry-event (&optional (topic "/some/topic") (display-name "Something Happened"))
  (make-instance 'telemetry-event
                 :topic topic
                 :display-name display-name
                 :timestamp (current-time)
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
                   :timestamp (current-time)
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
      (broadcast-event event))))

(defun start-telemetry ()
  (db-setup *db-file*)
  (format t "~%SETUP!!!~%")
  (bb:alet ((conn (mqtt:connect
                   *mqtt-host*
                   :port *mqtt-port*
                   :on-message
                   #'(lambda (message)
                       (handle-mqtt-message (mqtt:mqtt-message-topic message)
                                            (babel:octets-to-string
                                             (mqtt:mqtt-message-payload message)
                                             :encoding :utf-8
                                             :errorp nil))))))
    (setf *mqtt* conn)
    (dolist (topic *subscribe-topics*)
      (mqtt:subscribe conn topic 2))))

;;;; web

;; FIXME
(eval-when (:compile-toplevel :load-toplevel :execute)
  (wookie:load-plugins))

(defparameter *public-dir*
  (uiop:merge-pathnames* #p"public/" ctelemetry-base-config:*base-directory*))

(defun setup-public ()
  ;; FIXME: shouldn't require NAMESTRING
  (wookie-plugin-export:def-directory-route
      "/" (namestring *public-dir*)))

(setup-public)

#++
(wookie:defroute (:get "/events") (request response)
  (declare (ignore request))
  (wookie:send-response
   response
   :headers '(:content-type "application/json; charset=utf-8")
   :body (st-json:write-json-to-string
          (execute-to-list 'get-events))))

(wookie:defroute (:get "/sections") (request response)
  (wookie:send-response
   response
   :headers '(:content-type "application/json; charset=utf-8")
   :body (st-json:write-json-to-string *sections*)))

(wookie:defroute (:get "/latest(/.*)?") (request response args)
  (declare (ignore request))
  (wookie:send-response
   response
   :headers '(:content-type "application/json; charset=utf-8")
   :body (st-json:write-json-to-string
          (iter (for (topic topic-display-name cell-name cell-display-name count ts value)
                     in (execute-to-list 'get-latest
                                         :topic-pattern
                                         (concatenate 'string
                                                      (first args) ;; this handles NIL case, too
                                                      "%")))
                (collect
                    (list topic
                          (or (gethash topic *topic-overrides*) topic-display-name)
                          cell-name
                          (or (gethash (cons topic cell-name) *cell-overrides*) cell-display-name)
                          count ts
                          (or (ignore-errors (with-standard-io-syntax
                                               (let ((*read-eval* nil))
                                                 (read-from-string value))))
                              value)))))))

(wookie:defroute (:get "/ct(/.*)?") (request response)
  (declare (ignore request))
  (wookie:send-response
   response
   :headers '(:content-type "text/html; charset=utf-8")
   :body (alexandria:read-file-into-string
          (merge-pathnames #p"index.html" *public-dir*)
          :external-format :utf-8)))

(defvar *connected-clients* (make-hash-table))

(defun make-ws-server (req)
  (let ((ws (wsd:make-server req nil :type :wookie)))
    (setf (gethash ws *connected-clients*) t)
    (wsd:on :message ws
            #'(lambda (event)
                (log4cl:log-debug "~&inbound ws data: ~s~%" (wsd:event-data event))))
    (wsd:on :close ws
            #'(lambda (event)
                (log4cl:log-info "WebSocket client disconnected~@[: ~a~]"
                                 (wsd:event-reason event))
                (remhash ws *connected-clients*)))
    (wsd:start-connection ws)))

(wookie:defroute (:get "/ws-data") (req res)
  (declare (ignore res))
  (make-ws-server req))

(defun ws-broadcast (message)
  (iter (for (ws nil) in-hashtable *connected-clients*)
        (wsd:send ws (st-json:write-json-to-string message))))

(defun broadcast-event (event)
  (ws-broadcast
   (st-json:jso
    "topic" (topic event)
    "ts" (timestamp event)
    "cells" (iter (for (cell-name cell-value nil) in (cell-values event))
                  (collect (list (string-downcase cell-name) cell-value))))))

(defvar *web-listener* nil)

(defun start-web-server (&optional (port 8999))
  (unless *web-listener*
    (setf *web-listener* (make-instance 'wookie:listener :port port))
    (wookie:start-server *web-listener*)))

;; definitions

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


(defun config-path ()
  (uiop:merge-pathnames* *config-file-name* (user-homedir-pathname)))

(defun load-config ()
  (let ((path (config-path)))
    (if (probe-file path)
        (load path)
        (error "cannot load config file ~s" path))))

#+sbcl
(defun save-image ()
  #++
  (swank::swank-require
   '(:swank-repl :swank-asdf :swank-arglists :swank-fuzzy :swank-indentation
     :swank-fancy-inspector :swank-c-p-c :swank-util :swank-presentations
     :swank-listener-hooks))
  (setf *db* nil)
  (sb-ext:save-lisp-and-die
   "ctelemetry"
   :executable t
   :compression t
   :toplevel
   #'(lambda ()
       (load-config)
       (sb-ext:disable-debugger)
       (setf *public-dir*
             (merge-pathnames
              #p"public/"
              (uiop:pathname-directory-pathname sb-ext:*core-pathname*)))
       (setup-public) ;; FIXME
       (as:with-event-loop ()
         (start-telemetry)
         (start-web-server))
       #++
       (bt:make-thread
        #'(lambda ()
            (as:with-event-loop ()
              (start-telemetry)
              (start-web-server))))
       #++
       (sb-impl::toplevel-repl nil))))

;; TBD: event (topic) title overrides
;; TBD: fix timestamps coming from dscope!!!!
;; TBD: handle telemetry-error -- and warn
;; TBD: check max payload size
;; TBD: plain parallel running won't cut it --
;; should not use the same connection from multiple threads.
;; so, need to pick a connection from a pool and put it back later

#++
(store-telemetry-event (parse-event "/zzz/qqqrr" "(\"whatever\" 1421742558.099134d0 (:cell-one 42d0 \"cell one\") (:cell-two 43d0 \"cell two\"))"))
#++
(store-telemetry-event (parse-event "/zzz/qqq" "42d0"))
