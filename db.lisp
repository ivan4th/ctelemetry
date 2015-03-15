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
           #:db-version
           #:defddl
           #:defsql))

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

(defvar *table-ddl* (make-hash-table :test #'equal))
(defvar *db* nil)

(defun bind-params (params)
  (iter (for (name value) on params by #'cddr)
        (collect (if (stringp name)
                     name
                     (concatenate 'string ":"
                                  (substitute #\_ #\- (string-downcase name)))))
        (collect value)))

(macrolet ((def-exec-query (name sqlite-name)
             `(defun ,name (query &rest params)
                (apply #',sqlite-name *db* query (bind-params params)))))
  (def-exec-query execute-non-query sqlite:execute-non-query/named)
  (def-exec-query execute-to-list sqlite:execute-to-list/named)
  (def-exec-query execute-single sqlite:execute-single/named)
  (def-exec-query execute-one-row-m-v sqlite:execute-one-row-m-v/named))

(defun exec-and-get-rowid (query &rest params)
  (apply #'execute-non-query query params)
  (sqlite:last-insert-rowid *db*))

(defmacro defddl (name &body ddl)
  `(setf (gethash ',name *table-ddl*) (list ,@ddl)))

(defmacro defsql (name type sql)
  (with-gensyms (params)
    `(defun ,name (&rest ,params)
       (apply #',(ecase type
                   (:non-query 'execute-non-query)
                   (:list 'execute-to-list)
                   (:single 'execute-single)
                   (:row 'execute-one-row-m-v)
                   (:non-query-rowid 'exec-and-get-rowid))
              ,sql ,params))))

(defsql locate-a-table :row "select * from sqlite_master where type = 'table' and name = 'events'")
(defsql get-version :single "pragma user_version")

(defun required-db-version ()
  (if-let ((versions (hash-table-keys *db-versions*)))
    (reduce #'max versions)
    0))

(defun set-db-version (version)
  ;; cannot prepare "pragma user_version = :version"
  (execute-non-query
   (format nil "pragma user_version = ~a" version)))

(defun maybe-upgrade-db ()
  (let ((db-version (get-version))
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
    (cond ((locate-a-table)
           (maybe-upgrade-db))
	  (t
	   (log4cl:log-info "creating db schema from scratch")
	   (dolist (ddl-command (mappend #'cdr
                                         (sort (hash-table-alist *table-ddl*)
                                               #'string< :key #'car)))
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

(defmacro with-db-transaction (&body body)
  `(sqlite:with-transaction *db* ,@body))
