(defpackage :ctelemetry/db-commands
  (:import-from :ctelemetry/db :defddl :defsql)
  (:use :cl :alexandria :iterate)
  (:export #:get-latest
           #:load-event
           #:ensure-topic
           #:ensure-topic-cell
           #:store-event
           #:store-event-value
           #:update-cell
           #:get-events
           #:get-topics))

(in-package :ctelemetry/db-commands)

(defddl :ctelemetry
  "create table if not exists topics (
        id integer not null primary key autoincrement,
        topic text not null,
        display_name text)"
  "create unique index if not exists topics_topic on topics (topic)"
  "create table if not exists topic_cells (
        id integer not null primary key autoincrement,
        topic_id integer not null references topics(id),
        name text,
        display_name text,
        timestamp real,
        count integer,
        value text)"
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
  "create index if not exists event_values_cell_id on event_values(cell_id)")

(defsql get-latest :list
  "select t.topic, t.display_name as topic_display_name,
     tc.name as cell_name, tc.display_name as cell_display_name,
     tc.count, tc.timestamp as ts, tc.value
   from topics t
     join topic_cells tc on tc.topic_id = t.id
   where t.topic like :topic_pattern
   order by topic_display_name, cell_name")

(defsql load-event :list
  "select t.topic, t.display_name as topic_display_name,
     tc.name as cell_name, tc.display_name as cell_display_name,
     e.timestamp as ts, ev.value
   from events e
     join topics t on e.topic_id = t.id
     left join event_values ev on e.id = ev.event_id
     left join topic_cells tc on ev.cell_id = tc.id
   where e.id = :id
   order by cell_name")

(defsql ensure-topic :non-query-rowid
  "insert or replace into topics (id, topic, display_name)
   values ((select id from topics where topic = :topic), :topic, :display_name)")

(defsql ensure-topic-cell :non-query-rowid
  "insert or replace into topic_cells (id, topic_id, name, display_name, timestamp, count, value)
   values ((select id from topic_cells where topic_id = :topic_id and name = :name),
           :topic_id, :name, :display_name,
           (select timestamp from topic_cells where topic_id = :topic_id and name = :name),
           coalesce((select count from topic_cells where topic_id = :topic_id and name = :name), 0),
           (select value from topic_cells where topic_id = :topic_id and name = :name))")

(defsql store-event :non-query-rowid
  "insert into events (timestamp, topic_id) values (:timestamp, :topic_id)")

(defsql store-event-value :non-query
  "insert into event_values (event_id, cell_id, value)
   values (:event_id, :cell_id, :value)")

(defsql update-cell :non-query
  "update topic_cells set timestamp = :timestamp, count = count + 1, value = :value
   where topic_id = :topic_id and id = :cell_id")

(defsql get-events/no-filter :list
  "select e.id, e.timestamp, e.topic_id
   from events e
   join topics t on e.topic_id = t.id
   where t.topic like :topic_pattern and e.timestamp >= :start
   order by e.id desc limit :count")

(defsql get-topics :list
  "select id, topic, display_name from topics
   where topic like :topic_pattern
   order by display_name")

(defsql topic-list :list
  "select id from topics where topic like :topic_pattern")

(defun get-events (&key count topic-ids topic-pattern start)
  (assert (every #'integerp topic-ids))
  (if (null topic-ids)
      (get-events/no-filter :topic-pattern topic-pattern :count count :start start)
      (let ((actual-topic-ids
              (intersection (mapcar #'first (topic-list :topic-pattern topic-pattern))
                            topic-ids)))
        (ctelemetry/db:execute-to-list
         (with-standard-io-syntax
           (format nil "select e.id, e.timestamp, e.topic_id ~
                        from events e ~
                        where e.topic_id in (~{~d~^,~}) and e.timestamp >= :start
                        order by e.id desc limit :count"
                   actual-topic-ids))
         :count count
         :start start))))

;; "2015-01-30 10:05:48"
#++
(defparameter *sql-commands*
  '((:get-avg
     "select
          substr(datetime(e.timestamp, 'unixepoch', 'localtime'), 1, :ofs) ||
            substr('2015-01-01 00:00:00', :ofs + 1) ts,
          min(cast(ev.value as real)) v
        from event_values ev
          join events e on ev.event_id = e.id
        where ev.cell_id = :cell_id
        group by ts
        order by ts
        limit 10000")))

(defparameter *avg-pos*
  '((:avg-sec . 19)
    (:avg-min . 16)
    (:avg-hour . 13)
    (:avg-day . 10)
    (:avg-month . 7)
    (:avg-year . 4)))
