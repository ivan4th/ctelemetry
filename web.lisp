(defpackage :ctelemetry/web
  (:import-from :wookie)
  (:import-from :log4cl)
  (:import-from :websocket-driver)
  (:import-from :puri)
  (:import-from :blackbird)
  (:use :cl :alexandria :iterate)
  (:export #:define-route
           #:ws-broadcast
           #:get-var
           #:invoke-route
           ;; FIXME
           #:setup-public
           #:*public-dir*))

(in-package :ctelemetry/web)

(defparameter *www-port* 8999)
(defparameter *public-dir*
  (uiop:merge-pathnames* #p"public/" ctelemetry-base-config:*base-directory*))
(defvar *connected-clients* (make-hash-table))
(defvar *defined-routes* (make-hash-table))
(defvar *query-var-func*)

(pushnew '*query-var-func* bb:*promise-keep-specials*)

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
