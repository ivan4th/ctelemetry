(defpackage :ctelemetry/routes
  (:import-from :ctelemetry/db)
  (:import-from :ctelemetry/web)
  (:import-from :ctelemetry/event)
  (:use :cl :alexandria :iterate))

(in-package :ctelemetry/routes)

(ctelemetry/web:define-route sections (:get "/sections") ()
  (st-json:jso "sections" ctelemetry/event:*sections*))

(ctelemetry/web:define-route latest (:get "/latest(/.*)?") (args)
  (iter (for (topic topic-display-name cell-name cell-display-name count ts value)
             in (ctelemetry/db-commands:get-latest
                 :topic-pattern (concatenate 'string
                                             (first args) ;; this handles NIL case, too
                                             "%")))
        (collect
            (list topic
                  (ctelemetry/event:topic-display-name topic topic-display-name)
                  cell-name
                  (ctelemetry/event:cell-display-name topic cell-name cell-display-name)
                  count ts
                  (or (ignore-errors (with-standard-io-syntax
                                       (let ((*read-eval* nil))
                                         (read-from-string value))))
                      value)))))
