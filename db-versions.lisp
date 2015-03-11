(defpackage :ctelemetry/db-versions
  (:import-from :ctelemetry/db)
  (:use :cl))

(in-package :ctelemetry/db-versions)

(ctelemetry/db:db-version (1)
  "alter table topic_cells add timestamp real"
  "alter table topic_cells add count integer"
  "alter table topic_cells add value text"
  ;; kill records with broken timestamps
  "delete from event_values where event_id in (select id from events where timestamp < 1000000000)"
  "delete from events where timestamp < 1000000000"
  ;; update current cell values
  "update topic_cells set
    timestamp = (
      select e.timestamp from event_values ev
      join events e on ev.event_id = e.id
      where ev.cell_id = topic_cells.id
      order by e.id desc limit 1),
    count = (
      select count(*) from event_values ev
      join events e on ev.event_id = e.id
      where ev.cell_id = topic_cells.id),
    value = (
      select ev.value from event_values ev
      join events e on ev.event_id = e.id
      where ev.cell_id = topic_cells.id
      order by e.id desc limit 1)")
