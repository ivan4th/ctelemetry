(defpackage :ctelemetry/db
  (:import-from :sqlite)
  (:use :cl :alexandria :iterate)
  (:export #:db-setup
           #:db-disconnect
           #:execute-non-query
           #:execute-to-list
           #:execute-single
           #:execute-one-row-m-v
           #:exec-and-get-rowid
           #:with-db-transaction
           #:db-version))

(in-package :ctelemetry/db)

(defvar *db-versions* (make-hash-table))

(defmacro db-version ((n) &body body)
  (assert body () "no body specified for db version")
  (if (and body (every #'stringp body))
      `(db-version (,n)
	 ,@(iter (for item in body)
		 (collect `(execute-non-query ,item))))
      (let ((func-name (symbolicate 'db-version- (princ-to-string n))))
	`(progn
	   (defun ,func-name () ,@body)
	   (setf (gethash ,n *db-versions*) ',func-name)))))

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
  '((:get-version "pragma user_version")
    (:locate-a-table
     "select * from sqlite_master where type = 'table' and name = 'events'")
    (:ensure-topic
     "insert or replace into topics (id, topic, display_name)
      values ((select id from topics where topic = :topic), :topic, :display_name)")
    (:ensure-topic-cell
     "insert or replace into topic_cells (id, topic_id, name, display_name, timestamp, count, value)
      values ((select id from topic_cells where topic_id = :topic_id and name = :name),
              :topic_id, :name, :display_name,
              (select timestamp from topic_cells where topic_id = :topic_id and name = :name),
              coalesce((select count from topic_cells where topic_id = :topic_id and name = :name), 0),
              (select value from topic_cells where topic_id = :topic_id and name = :name))")
    (:store-event
     "insert into events (timestamp, topic_id) values (:timestamp, :topic_id)")
    (:store-event-value
     "insert into event_values (event_id, cell_id, value)
      values (:event_id, :cell_id, :value)")
    (:update-cell
     "update topic_cells set timestamp = :timestamp, count = count + 1, value = :value
      where topic_id = :topic_id and id = :cell_id")
    (:get-events
     "select t.topic, t.display_name, e.id, e.timestamp, e.topic_id
      from events e join topics t on e.topic_id = t.id
      order by e.id desc limit 100")
    (:get-latest
     "select t.topic, t.display_name as topic_display_name,
             tc.name as cell_name, tc.display_name as cell_display_name,
             tc.count, tc.timestamp as ts, tc.value
      from topics t
           join topic_cells tc on tc.topic_id = t.id
      where t.topic like :topic_pattern
      order by topic_display_name, cell_name")))

(defvar *db* nil)

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

(defun required-db-version ()
  (if-let ((versions (hash-table-keys *db-versions*)))
    (reduce #'max versions)
    0))

(defun set-db-version (version)
  ;; cannot prepare "pragma user_version = :version"
  (execute-non-query
   (format nil "pragma user_version = ~a" version)))

(defun maybe-upgrade-db ()
  (let ((db-version (execute-single :get-version))
	(required-version (required-db-version)))
    (cond ((> db-version required-version)
	   (log4cl:log-warn "db has version ~a which is newer than required version ~a"
                            db-version required-version))
	  ((< db-version required-version)
	   (log4cl:log-info "db version ~a, required version ~a"
                            db-version required-version)
	   (iter (for version from (1+ db-version) to required-version)
		 (when-let ((upgrade-func (gethash version *db-versions*)))
		   (log4cl:log-info "upgrading db to version ~a" version)
		   (funcall upgrade-func)
		   (set-db-version version))))
	  (t
	   (log4cl:log-info "no db upgrade required")))))

(defun ensure-schema ()
  (sqlite:with-transaction *db*
    (cond ((execute-single :locate-a-table)
           (maybe-upgrade-db))
	  (t
	   (log4cl:log-info "creating db schema from scratch")
	   (dolist (ddl-command *table-ddl*)
             (:printv ddl-command)
	     (execute-non-query ddl-command))
           (set-db-version (required-db-version))))))

(defun db-setup (&optional (db-file ":memory:"))
  (format t "~%DB: ~s~%" *db*)
  (unless *db*
    (setf *db* (sqlite:connect db-file))
    (format t "~%DB CONN: ~s~%" *db*)
    (ensure-schema)))

(defun db-disconnect ()
  (if *db*
      (sqlite:disconnect (shiftf *db* nil))
      (log4cl:log-warn "already disconnected")))

(defun exec-and-get-rowid (query &rest params)
  (apply #'execute-non-query query params)
  (sqlite:last-insert-rowid *db*))

(defmacro with-db-transaction (&body body)
  `(sqlite:with-transaction *db* ,@body))
