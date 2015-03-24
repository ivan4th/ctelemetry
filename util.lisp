(defpackage :ctelemetry/util
  (:import-from :babel)
  (:import-from :ironclad)
  (:use :cl :alexandria)
  (:export #:hmac-append-auth
           #:hmac-verify
           #:current-time))

(in-package :ctelemetry/util)

(define-constant +hmac-len+ 40)

(defun make-hmac-key (str)
  (ironclad:digest-sequence :sha1 (babel:string-to-octets str)))

(defun hmac-digest (str key-str)
  (let ((hmac (ironclad:make-hmac (make-hmac-key key-str) :sha1)))
    (ironclad:update-hmac hmac (babel:string-to-octets str))
    (ironclad:byte-array-to-hex-string (ironclad:hmac-digest hmac))))

(defun hmac-append-auth (str key-str)
  (concatenate 'string (hmac-digest str key-str) str))

(defun hmac-verify (str key-str)
  (when (and (>= (length str) +hmac-len+)
             (not (find-if-not #'(lambda (c)
                                   (or (digit-char-p c)
                                       (char<= #\A c #\F)
                                       (char<= #\a c #\f)))
                               str :end +hmac-len+)))
    (let ((data (subseq str +hmac-len+)))
      (when (string= (subseq str 0 +hmac-len+)
                     (hmac-digest data key-str))
        data))))

(defun current-time ()
  (i4-diet-utils:universal-time->unix-timestamp
   (get-universal-time)))
