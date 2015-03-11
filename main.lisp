(defpackage :ctelemetry/main
  (:import-from :blackbird)
  (:import-from :cl-async)
  (:import-from :cl-async-repl)
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
  (:import-from :ctelemetry/db)
  (:use :cl :alexandria :iterate))

(in-package :ctelemetry/main)

(defparameter *config-file-name* #p".ctelemetryrc")

(defparameter *max-payload-size* 8192)
(defparameter *db-file* #p"/var/lib/ctelemetry/telemetry.db")
(defparameter *mqtt-host* "localhost")
(defparameter *mqtt-port* 1883)
(defparameter *www-port* 8999)
(defparameter *mqtt-retry-delay-seconds* 5)
(defvar *mqtt* nil)
(defvar *mqtt-reconnect-timer* nil)
(defvar *topic-overrides* (make-hash-table :test #'equal))
(defvar *cell-overrides* (make-hash-table :test #'equal))
(defvar *subscribe-topics* '())
(defvar *sections* '())

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

(defun store-telemetry-event (event)
  (ctelemetry/db:with-db-transaction
    (let* ((topic-id (ctelemetry/db:exec-and-get-rowid
                      :ensure-topic
                      :topic (topic event)
                      :display-name (display-name event)))
           (event-id (ctelemetry/db:exec-and-get-rowid
                      :store-event
                      :timestamp (timestamp event)
                      :topic-id topic-id)))
      (iter (for (cell-name cell-value cell-display-name) in (cell-values event))
            (let ((cell-id (ctelemetry/db:exec-and-get-rowid
                            :ensure-topic-cell
                            :topic-id topic-id
                            :name (string-downcase cell-name)
                            :display-name cell-display-name)))
              (ctelemetry/db:execute-non-query
               :store-event-value
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

(defun try-connecting ()
  (labels ((later ()
             (unless *mqtt-reconnect-timer*
               (log4cl:log-info "waiting for ~a seconds before retrying the connection"
                                *mqtt-retry-delay-seconds*)
               (setf *mqtt-reconnect-timer*
                     (as:with-delay (*mqtt-retry-delay-seconds*)
                       (setf *mqtt-reconnect-timer* nil)
                       (try-connecting)))))
           (connect ()
             (log4cl:log-info "trying to connect to MQTT broker at ~a:~a"
                              *mqtt-host* *mqtt-port*)
             (bb:chain
                 (mqtt:connect *mqtt-host*
                               :port *mqtt-port*
                               :on-message
                               #'(lambda (message)
                                   (handle-mqtt-message (mqtt:mqtt-message-topic message)
                                                        (babel:octets-to-string
                                                         (mqtt:mqtt-message-payload message)
                                                         :encoding :utf-8
                                                         :errorp nil)))
                               :error-handler
                               #'(lambda (c)
                                   (setf *mqtt* nil)
                                   (log4cl:log-warn "MQTT error: ~a" c)
                                   (later))
                               :clean-session nil)
               (:then (conn)
                 (log4cl:log-info "connected to MQTT broker")
                 (setf *mqtt* conn)
                 (dolist (topic *subscribe-topics*)
                   (mqtt:subscribe conn topic 2)))
               (:catch (c)
                 (log4cl:log-warn "MQTT connecting failed: ~a" c)
                 (later)))))
    (cond (*mqtt*
           (log4cl:log-info "already connected"))
          (*mqtt-reconnect-timer*
           (log4cl:log-info "already trying to connect"))
          (t
           (connect)))))

(defun start-telemetry ()
  (ctelemetry/db:db-setup *db-file*)
  (format t "~%SETUP!!!~%")
  (try-connecting))

(defun stop-telemetry ()
  (when *mqtt*
    (mqtt:disconnect *mqtt*)
    (setf *mqtt* nil))
  (when *mqtt-reconnect-timer*
    (as:free-event *mqtt-reconnect-timer*)
    (setf *mqtt-reconnect-timer* nil)))

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
          (ctelemetry/db:execute-to-list :get-events))))

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
                 in (ctelemetry/db:execute-to-list
                     :get-latest
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

#++
(store-telemetry-event (parse-event "/zzz/qqqrr" "(\"whatever\" 1421742558.099134d0 (:cell-one 42d0 \"cell one\") (:cell-two 43d0 \"cell two\"))"))
#++
(store-telemetry-event (parse-event "/zzz/qqq" "42d0"))
