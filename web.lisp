(defpackage :ctelemetry/web
  (:import-from :wookie)
  (:import-from :log4cl)
  (:import-from :websocket-driver)
  (:use :cl :alexandria :iterate)
  (:export #:define-route
           #:ws-broadcast
           ;; FIXME
           #:setup-public
           #:*public-dir*))

(in-package :ctelemetry/web)

(defparameter *www-port* 8999)

;; FIXME
(eval-when (:compile-toplevel :load-toplevel :execute)
  (wookie:load-plugins))

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

(wookie:defroute (:get "/ct(/.*)?") (request response)
  (declare (ignore request))
  (wookie:send-response
   response
   :headers '(:content-type "text/html; charset=utf-8")
   :body (alexandria:read-file-into-string
          (merge-pathnames #p"index.html" *public-dir*)
          :external-format :utf-8)))

(wookie:defroute (:get "/ws-data") (req res)
  (declare (ignore res))
  (make-ws-server req))

(defun ws-broadcast (message)
  (iter (for (ws nil) in-hashtable *connected-clients*)
        (wsd:send ws (st-json:write-json-to-string message))))

(defparameter *public-dir*
  (uiop:merge-pathnames* #p"public/" ctelemetry-base-config:*base-directory*))

(defun setup-public ()
  ;; FIXME: shouldn't require NAMESTRING
  (wookie-plugin-export:def-directory-route
      "/" (namestring *public-dir*)))

(setup-public)

(defmacro define-route (name (method resource &rest options) (&optional args-var) &body body)
  (let ((func-name (symbolicate 'web-route- name))
        (maybe-args (when args-var (list args-var))))
    (with-gensyms (request response)
      `(progn
         (defun ,func-name (,@maybe-args) ,@body)
         (wookie:defroute (,method ,resource ,@options)
             (,request ,response ,@maybe-args)
           (declare (ignore ,request))
           (wookie:send-response
            ,response
            :headers '(:content-type "application/json; charset=utf-8")
            :body (st-json:write-json-to-string (,func-name ,@maybe-args))))))))

;; TBD: tickets (for auth)
