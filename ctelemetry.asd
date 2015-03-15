#-asdf3 (error "walt requires ASDF 3")
(defsystem #:ctelemetry
  :class :package-inferred-system
  :defsystem-depends-on (:asdf-package-system)
  :depends-on (:ctelemetry/main)
  :in-order-to ((test-op (load-op :ctelemetry/test)))
  :perform (test-op (o c) (symbol-call :vtf :run-tests :ctelemetry/test)))

(defpackage #:ctelemetry-base-config (:export #:*base-directory*))
(defparameter ctelemetry-base-config:*base-directory*
  (make-pathname :name nil :type nil :defaults *load-truename*))
