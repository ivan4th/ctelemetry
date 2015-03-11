(defpackage :ctelemetry/db
  (:import-from :sqlite)
  (:use :cl :alexandria :iterate)
  (:export #:db-setup
           #:execute-non-query
           #:execute-to-list
           #:execute-single
           #:execute-one-row-m-v
           #:exec-and-get-rowid
           #:with-db-transaction))

(in-package :ctelemetry/db)

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
  '((:ensure-topic
     "insert or replace into topics (id, topic, display_name)
      values ((select id from topics where topic = :topic), :topic, :display_name)")
    (:ensure-topic-cell
     "insert or replace into topic_cells (id, topic_id, name, display_name)
      values ((select id from topic_cells where topic_id = :topic_id and name = :name),
              :topic_id, :name, :display_name)")
    (:store-event
     "insert into events (timestamp, topic_id) values (:timestamp, :topic_id)")
    (:store-event-value
     "insert into event_values (event_id, cell_id, value)
      values (:event_id, :cell_id, :value)")
    (:get-events
     "select t.topic, t.display_name, e.id, e.timestamp, e.topic_id
      from events e join topics t on e.topic_id = t.id
      order by e.id desc limit 100")
    (:get-latest
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

(defun exec-and-get-rowid (query &rest params)
  (apply #'execute-non-query query params)
  (sqlite:last-insert-rowid *db*))

(defmacro with-db-transaction (&body body)
  `(sqlite:with-transaction *db* ,@body))
