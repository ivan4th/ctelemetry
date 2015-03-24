(defpackage :ctelemetry/web
  (:import-from :cl-async)
  (:import-from :wookie)
  (:import-from :log4cl)
  (:import-from :websocket-driver)
  (:import-from :puri)
  (:import-from :cl-who)
  (:import-from :blackbird)
  (:import-from :ctelemetry/util)
  (:use :cl :alexandria :iterate)
  (:export #:define-route
           #:ws-broadcast
           #:get-var
           #:invoke-route
           ;; FIXME
           #:setup-public
           #:*public-dir*
           #:*auth-key*))

(in-package :ctelemetry/web)

(defparameter *www-port* 8999)
(defparameter *public-dir*
  (uiop:merge-pathnames* #p"public/" ctelemetry-base-config:*base-directory*))
(defparameter *auth-token-validity-duration-sec* 600)
(defparameter *auth-timeout-seconds* 20)
(defvar *connected-clients* (make-hash-table))
(defvar *defined-routes* (make-hash-table))
(defvar *query-var-func*)
(defvar *auth-key* nil)

(defclass ws-client ()
  ((connect-ts :accessor connect-ts :initarg :connect-ts)
   (authenticated-p :accessor authenticated-p :initform nil)
   (websocket :accessor websocket :initarg :websocket)))

(pushnew '*query-var-func* bb:*promise-keep-specials*)

(defun require-auth-key ()
  (or *auth-key* (error "no auth key defined")))

(defun gen-auth-token (&optional (time (ctelemetry/util:current-time)))
  (ctelemetry/util:hmac-append-auth
   (with-standard-io-syntax
     (format nil "~(~16,'0x~)" time))
   (require-auth-key)))

(defun verify-auth-token (token)
  (when-let ((time (ctelemetry/util:hmac-verify token (require-auth-key))))
    (when-let ((parsed (ignore-errors (parse-integer time :radix 16))))
      (let ((cur-time (ctelemetry/util:current-time)))
        (<= 0 (- cur-time parsed) *auth-token-validity-duration-sec*)))))

;; FIXME
(eval-when (:compile-toplevel :load-toplevel :execute)
  ;; FIXME: When trying to re-load plugins, most plugins
  ;; are unloaded. Also, maybe loading them this way
  ;; isn't correct either.
  (defvar *plugins-loaded* nil)
  (unless (shiftf *plugins-loaded* t)
    (wookie:load-plugins)))

(defun get-var (name)
  (funcall *query-var-func* name))

(defun make-ws-server (req)
  (let* ((ws (wsd:make-server req nil :type :wookie))
         (wsc (make-instance 'ws-client
                             :connect-ts (ctelemetry/util:current-time)
                             :websocket ws))
         disconnect-timer)
    (flet ((close-connection ()
             (wsd:close-connection ws)
             (remhash wsc *connected-clients*)))
      (setf disconnect-timer
            (as:with-delay (*auth-timeout-seconds*)
              (log4cl:log-info "client auth timed out")
              (close-connection)))
      (setf (gethash wsc *connected-clients*) t)
      (wsd:on :message ws
              #'(lambda (event)
                  (:printv (wsd:event-data event))
                  (log4cl:log-debug "inbound ws data: ~s" (wsd:event-data event))
                  (cond ((authenticated-p wsc))
                        ((not (stringp (wsd:event-data event)))
                         (log4cl:log-info "text data expected on the socket")
                         (close-connection))
                        ((not (verify-auth-token (wsd:event-data event)))
                         (log4cl:log-info "socket auth failed")
                         (close-connection))
                        (t
                         (log4cl:log-info "websocket connection authenticated")
                         (setf (authenticated-p wsc) t)
                         (as:free-event disconnect-timer)))))
      (wsd:on :close ws
              #'(lambda (event)
                  (log4cl:log-info "WebSocket client disconnected~@[: ~a~]"
                                   (wsd:event-reason event))
                  (remhash wsc *connected-clients*)))
      (wsd:start-connection ws))))

(wookie:defroute (:get "^(/|/ct(/.*)?)$" :priority 1) (request response)
  (declare (ignore request))
  (wookie:send-response
   response
   :headers '(:content-type "text/html; charset=utf-8")
   :body (concatenate
          'string
          "<!doctype html>"
          (cl-who:with-html-output-to-string (out nil :indent t)
            (:html
             (:head
              (:meta :http-equiv "Content-Type" :content "text/html; charset=utf-8")
              (:base :href "/")
              (:title "Common Telemetry")
              (:link :rel "stylesheet" :href "dist/style.css")
              (:script :src "http://localhost:35729/livereload.js")
              (:script :src "dist/share.js"))
             (:body
              :data-auth (gen-auth-token)
              :ng-app "ctelemetryApp"
              (:div :ng-include (cl-who:escape-string "'views/navbar.html'"))
              (:div :class "container primary-content" :ng-view "")
              (:div :ng-include (cl-who:escape-string "'views/common.html'"))))))))

(wookie:defroute (:get "/ws-data") (req res)
  (declare (ignore res))
  (make-ws-server req))

(defun ws-broadcast (message)
  (iter (for (wsc nil) in-hashtable *connected-clients*)
        (when (authenticated-p wsc)
          (wsd:send (websocket wsc)
                    (st-json:write-json-to-string message)))))

(defun setup-public ()
  ;; FIXME: shouldn't require NAMESTRING
  (wookie-plugin-export:def-directory-route
      "/" (namestring *public-dir*)
    :disable-directory-listing t))

(setup-public)

(defmacro define-route (name (method resource &rest options &key (priority nil priority-p))
                        (&optional args-var) &body body)
  (declare (ignore priority)) ;; included in options if present
  (let ((func-name (symbolicate 'web-route- name)))
    (with-gensyms (request response var-name)
      `(progn
         ,(if args-var
              `(defun ,func-name (,args-var) ,@body)
              (with-gensyms (dummy)
                `(defun ,func-name (,dummy)
                   (declare (ignore ,dummy))
                   ,@body)))
         (wookie:defroute (,method ,resource ,@options
                           ;; priority defaults to 1 to override the / route
                           ,@(unless priority-p '(:priority 1)))
             (,request ,response ,@(when args-var (list args-var)))
           (let ((*query-var-func*
                   #'(lambda (,var-name)
                       (wookie-plugin-export:get-var ,request ,var-name))))
             (wookie:send-response
              ,response
              :headers '(:content-type "application/json; charset=utf-8")
              :body (st-json:write-json-to-string
                     ;; ,args-var may be NIL
                     (,func-name ,args-var)))))
         (setf (gethash ',name *defined-routes*)
               (list ',method ,resource ',func-name))))))

(defun invoke-route (method url)
  "Invoke a route corresponding to URL using METHOD"
  ;; TBD: post params
  (let* ((uri (puri:parse-uri url))
         ;; WOOKIE-UTIL:QUERYSTRING-TO-HASH accepts NIL, too
         (vars (wookie-util:querystring-to-hash (puri:uri-query uri)))
         (*query-var-func* #'(lambda (name)
                               (gethash name vars))))
    (iter (for (route-method route-resource route-func)
           in (hash-table-values *defined-routes*))
          (when (eq route-method method)
            (multiple-value-bind (whole-match groups)
                (cl-ppcre:scan-to-strings route-resource (puri:uri-path uri))
              (when whole-match
                (return (funcall route-func (coerce groups 'list))))))
          (finally
              (error "route not found for url: ~s" url)))))

;; TBD: tickets (for auth)
;; TBD: use angular-websocket
;; TBD: send updated ticket to clients from time to time
;; via separate 'update-ticket' message so that the client may reconnect
;; upon connection breakage
;; TBD: ensure websocket
