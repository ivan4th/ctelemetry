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
  (:import-from :ctelemetry/db)
  (:import-from :ctelemetry/db-versions)
  (:import-from :ctelemetry/event
                ;; FIXME
                #:*topic-overrides*
                #:*cell-overrides*
                #:*subscribe-topics*
                #:define-section
                #:define-topic
                #:add-subscription-topic)
  (:import-from :ctelemetry/web
                ;; FIXME
                #:*www-port*
                #:*auth-key*
                #:*http-auth*)
  (:import-from :ctelemetry/routes)
  (:use :cl :alexandria :iterate))

(in-package :ctelemetry/main)

(defparameter *config-file-name* #p".ctelemetryrc")

(defparameter *db-file* #p"/var/lib/ctelemetry/telemetry.db")
(defparameter *mqtt-host* "localhost")
(defparameter *mqtt-port* 1883)
(defparameter *mqtt-retry-delay-seconds* 5)
(defvar *mqtt* nil)
(defvar *mqtt-reconnect-timer* nil)

(setf ctelemetry/event:*event-broadcast-function* 'ctelemetry/web:ws-broadcast)

(defun ppr (value &optional title)
  (bb:attach-errback
   (bb:attach value
              #'(lambda (&rest values)
                  (format t "~&*** PROMISE~@[ ~a~] RESULT: ~{~s~^ ~}"
                          title values)))
   #'(lambda (e)
       (format t "~&*** PROMISE~@[ ~a~] ERROR: ~a: ~a"
               title (type-of e) e))))

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
                                   (ctelemetry/event:handle-mqtt-message
                                    (mqtt:mqtt-message-topic message)
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
    (as:remove-event *mqtt-reconnect-timer*)
    (as:free-event *mqtt-reconnect-timer*)
    (setf *mqtt-reconnect-timer* nil))
  (ctelemetry/db:db-disconnect))

;;;; web

#++
(wookie:defroute (:get "/events") (request response)
  (declare (ignore request))
  (wookie:send-response
   response
   :headers '(:content-type "application/json; charset=utf-8")
   :body (st-json:write-json-to-string
          (ctelemetry/db:execute-to-list :get-events))))

#++ ;; TBD: rm!!! (isn't needed, just for reference here)
(defun broadcast-event (event)
  (ws-broadcast
   (st-json:jso
    "topic" (topic event)
    "displayName" (display-name event)
    "ts" (timestamp event)
    "cells" (iter (for (cell-name cell-value nil) in (cell-values event))
                  (collect (list (string-downcase cell-name) cell-value))))))

(defvar *web-listener* nil)

(defun start-web-server (&optional (port *www-port*))
  (unless *web-listener*
    (setf *web-listener* (make-instance 'wookie:listener :port port))
    (wookie:start-server *web-listener*)))

;;; config

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
       (setf ctelemetry/web:*public-dir*
             (merge-pathnames
              #p"public/"
              (uiop:pathname-directory-pathname sb-ext:*core-pathname*)))
       (ctelemetry/web:setup-public) ;; FIXME
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

;; TBD: energy -- dose rate graph
;; TBD: don't connect points

;; TBD: разбить мощноcть дозы по энергиям

#++
(store-telemetry-event (parse-event "/zzz/qqqrr" "(\"whatever\" 1421742558.099134d0 (:cell-one 42d0 \"cell one\") (:cell-two 43d0 \"cell two\"))"))
#++
(store-telemetry-event (parse-event "/zzz/qqq" "42d0"))
