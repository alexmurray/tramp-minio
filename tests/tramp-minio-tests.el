;;; tramp-minio-tests.el --- Regression tests -*- lexical-binding: t; -*-
(require 'ert)
(require 'cl-lib)
(require 'tramp-minio)

(defconst tramp-minio-test-name "/minio:lab:/bucket/file.txt")
(defconst tramp-minio-test-row '((status . "success") (name . "file.txt") (type . "file") (size . 4) (etag . "test-etag") (lastModified . "2026-01-01T00:00:00Z")))

(defmacro tramp-minio-test-with-alias (&rest body)
  `(let ((tramp-minio--cache (make-hash-table :test 'equal))
         (tramp-minio-allowed-aliases nil))
     (cl-letf (((symbol-function 'tramp-minio--alias-info) (lambda () '(("lab" . "https://example.invalid")))))
       ,@body)))

(ert-deftest tramp-minio-handlers-defined ()
  (dolist (entry tramp-minio-file-name-handler-alist)
    (should (fboundp (cdr entry)))))

(ert-deftest tramp-minio-predicate ()
  (should (tramp-minio-file-name-p tramp-minio-test-name))
  (should (tramp-minio-file-name-p (tramp-dissect-file-name tramp-minio-test-name)))
  (should-not (tramp-minio-file-name-p "/tmp/local")))

(ert-deftest tramp-minio-target ()
  (tramp-minio-test-with-alias
   (should (equal (tramp-minio--target "/minio:lab:/bucket/a b") "lab/bucket/a b"))
   (should-error (tramp-minio--target "/minio:bad:/bucket/a") :type 'file-error)
   (should-error (tramp-minio--object-target "/minio:lab:/bucket/") :type 'file-error)
   (should-error (tramp-minio--target "/minio:lab:/bucket/a//b") :type 'file-error)))

(ert-deftest tramp-minio-listing-completion ()
  (tramp-minio-test-with-alias
   (cl-letf (((symbol-function 'tramp-minio--run)
              (lambda (&rest args)
                (should (equal args '("ls" "lab/bucket/")))
                '(((key . "file.txt") (type . "file") (size . 4))
                  ((key . "sub/") (type . "folder"))))))
     (should (equal (directory-files "/minio:lab:/bucket/") '("." ".." "file.txt" "sub")))
     (should (equal (file-name-all-completions "s" "/minio:lab:/bucket/") '("sub/")))
     (should-not (file-name-all-completions "F" "/minio:lab:/bucket/")))))

(ert-deftest tramp-minio-collision ()
  (tramp-minio-test-with-alias
   (cl-letf (((symbol-function 'tramp-minio--run)
              (lambda (&rest _) '(((key . "x") (type . "file")) ((key . "x/") (type . "folder"))))))
     (should-error (tramp-minio--entries "/minio:lab:/bucket/") :type 'file-error))))

(ert-deftest tramp-minio-stat-attributes ()
  (tramp-minio-test-with-alias
   (cl-letf (((symbol-function 'tramp-minio--stat) (lambda (_) tramp-minio-test-row)))
     (let ((attrs (file-attributes tramp-minio-test-name 'string)))
       (should (= (file-attribute-size attrs) 4))
       (should (stringp (file-attribute-user-id attrs)))
       (should-not (file-attribute-type attrs))))))

(ert-deftest tramp-minio-read-only-and-destructive ()
  (let ((tramp-minio-read-only t))
    (should-error (tramp-minio--write "test" nil tramp-minio-test-name) :type 'file-error)
    (should-error (tramp-minio--mkdir "/minio:lab:/bucket/") :type 'file-error))
  (let ((tramp-minio-read-only nil))
    (should-error (rename-file tramp-minio-test-name "/minio:lab:/bucket/" t) :type 'file-error)
    (should-error (rename-file tramp-minio-test-name "/minio:other:/bucket/file.txt" t) :type 'file-error)
    (should-error (delete-file tramp-minio-test-name) :type 'file-error)
    (should-error (delete-directory "/minio:lab:/bucket/" t) :type 'file-error)
    (should-error (tramp-minio--write "test" nil tramp-minio-test-name nil nil nil 'excl) :type 'file-error)))

(ert-deftest tramp-minio-binary-read ()
  (tramp-minio-test-with-alias
   (let ((payload (unibyte-string 0 255 13 10)))
     (cl-letf (((symbol-function 'tramp-minio--stat) (lambda (_) tramp-minio-test-row))
               ((symbol-function 'tramp-minio--check-program) #'ignore)
               ((symbol-function 'tramp-minio--process) (lambda (_args _timeout &optional _raw _in out) (let ((coding-system-for-write 'no-conversion)) (write-region payload nil out nil 'silent)) (list 0 "" ""))))
       (with-temp-buffer
         (set-buffer-multibyte nil)
         (let ((coding-system-for-read 'no-conversion))
           (insert-file-contents tramp-minio-test-name))
         (should (equal (buffer-string) payload)))))))

(ert-deftest tramp-minio-interrupted-transfer-cleans-up ()
  (tramp-minio-test-with-alias
   (let ((tmp (make-temp-file "minio-test-")))
     (unwind-protect
         (cl-letf (((symbol-function 'tramp-minio--stat) (lambda (_) tramp-minio-test-row))
                   ((symbol-function 'tramp-minio--temp) (lambda (&rest _) tmp))
                   ((symbol-function 'tramp-minio--check-program) #'ignore)
                   ((symbol-function 'tramp-minio--process) (lambda (&rest _) (signal 'quit nil))))
           (should (eq 'interrupted (condition-case nil (tramp-minio--download tramp-minio-test-name) (quit 'interrupted))))
           (should-not (file-exists-p tmp)))
       (when (file-exists-p tmp) (delete-file tmp))))))

(ert-deftest tramp-minio-structured-errors ()
  (cl-letf (((symbol-function 'tramp-minio--check-program) #'ignore)
            ((symbol-function 'tramp-minio--process) (lambda (&rest _) '(1 "{\"status\":\"error\",\"code\":\"NoSuchKey\"}" ""))))
    (should-error (tramp-minio--run "stat" "lab/bucket/file") :type 'tramp-minio-missing))
  (cl-letf (((symbol-function 'tramp-minio--check-program) #'ignore)
            ((symbol-function 'tramp-minio--process) (lambda (&rest _) '(1 "{\"status\":\"error\",\"code\":\"AccessDenied\"}" ""))))
    (should-error (tramp-minio--run "stat" "lab/bucket/file") :type 'file-error)))

(ert-deftest tramp-minio-cache ()
  (let ((tramp-minio--cache (make-hash-table :test 'equal)) (calls 0))
    (dotimes (_ 2) (tramp-minio--cached 'test 30 (lambda () (cl-incf calls))))
    (should (= calls 1))
    (tramp-minio-clear-cache)
    (tramp-minio--cached 'test 30 (lambda () (cl-incf calls)))
    (should (= calls 2))))

(ert-deftest tramp-minio-fake-client ()
  (skip-unless (and (executable-find "python3") (getenv "TRAMP_MINIO_FAKE_MC")))
  (let ((tramp-minio-program (getenv "TRAMP_MINIO_FAKE_MC"))
        (tramp-minio--checked-program nil)
        (tramp-minio--cache (make-hash-table :test 'equal)))
    (should (member "bucket" (directory-files "/minio:lab:/")))
    (should (member "file.txt" (directory-files "/minio:lab:/bucket/")))
    (with-temp-buffer
      (insert-file-contents tramp-minio-test-name)
      (should (equal (buffer-string) "test")))))

(ert-deftest tramp-minio-live-read ()
  (skip-unless (getenv "TRAMP_MINIO_TEST_OBJECT"))
  (let ((name (getenv "TRAMP_MINIO_TEST_OBJECT")))
    (should (file-regular-p name))
    (should (file-exists-p name))
    (with-temp-buffer (insert-file-contents-literally name) (should (>= (buffer-size) 0)))))

(ert-deftest tramp-minio-filename-fallback ()
  (should (stringp (tramp-minio-file-name-handler
                    'abbreviate-file-name tramp-minio-test-name)))
  (should (eq (tramp-minio-file-name-handler
               'get-file-buffer tramp-minio-test-name)
              (get-file-buffer tramp-minio-test-name))))

(ert-deftest tramp-minio-unsupported-operation-diagnostic ()
  (let ((err (should-error
              (tramp-minio-file-name-handler 'delete-file tramp-minio-test-name)
              :type 'file-error)))
    (should (memq 'delete-file err)))
  (should-error (tramp-minio-file-name-handler
                 'file-notify-add-watch tramp-minio-test-name '(change) #'ignore)
                :type 'file-notify-error))

(ert-deftest tramp-minio-visit-integration ()
  (skip-unless (getenv "TRAMP_MINIO_FAKE_MC"))
  (let ((tramp-minio-program (getenv "TRAMP_MINIO_FAKE_MC"))
        (tramp-minio--checked-program nil)
        (tramp-minio--cache (make-hash-table :test 'equal))
        (enable-local-variables nil) buffer)
    (unwind-protect
        (progn
          (setq buffer (find-file-noselect tramp-minio-test-name))
          (with-current-buffer buffer
            (should (equal (buffer-string) "test"))
            (should-not (buffer-modified-p))
            (should tramp-minio--visited-identity)
            (text-mode)
            (should tramp-minio--visited-identity)
            (should (verify-visited-file-modtime buffer))
            (should backup-inhibited)))
      (when (buffer-live-p buffer) (kill-buffer buffer)))))

(ert-deftest tramp-minio-dired-integration ()
  (skip-unless (getenv "TRAMP_MINIO_FAKE_MC"))
  (require 'dired)
  (let ((tramp-minio-program (getenv "TRAMP_MINIO_FAKE_MC"))
        (tramp-minio--checked-program nil) buffer)
    (unwind-protect
        (progn (setq buffer (dired-noselect "/minio:lab:/bucket/"))
               (with-current-buffer buffer
                 (should (string-match-p "file.txt" (buffer-string)))))
      (when (buffer-live-p buffer) (kill-buffer buffer)))))

(defun tramp-minio-test-save-and-revert (name initial)
  (unless (and (executable-find "python3") (getenv "TRAMP_MINIO_FAKE_MC"))
    (ert-skip "Set TRAMP_MINIO_FAKE_MC and install python3"))
  (let ((tramp-minio-program (getenv "TRAMP_MINIO_FAKE_MC"))
        (tramp-minio--checked-program nil)
        (tramp-minio--cache (make-hash-table :test 'equal))
        (tramp-minio-read-only nil)
        (tramp-minio-allow-overwrite t)
        (process-environment (copy-sequence process-environment))
        (enable-local-variables nil)
        (state (make-temp-file "tramp-minio-test-" t))
        (contents "Updated text\n")
        buffer)
    (unwind-protect
        (progn
          (setenv "TRAMP_MINIO_FAKE_STATE" state)
          (setq buffer (find-file-noselect name))
          (with-current-buffer buffer
            (should (equal (buffer-string) initial))
            (should (verify-visited-file-modtime buffer))
            (erase-buffer)
            (insert contents)
            (save-buffer)
            (should-not (buffer-modified-p))
            (should tramp-minio--needs-revert)
            (insert "Unsaved edit")
            (cl-letf (((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
              (should-error (save-buffer) :type 'file-error))
            (should (buffer-modified-p))
            (revert-buffer t t)
            (should (equal (buffer-string) contents))
            (should (verify-visited-file-modtime buffer))
            (should-not tramp-minio--needs-revert)))
      (when (buffer-live-p buffer)
        (with-current-buffer buffer (set-buffer-modified-p nil))
        (kill-buffer buffer))
      (delete-directory state t))))

(ert-deftest tramp-minio-save-existing-object ()
  (tramp-minio-test-save-and-revert tramp-minio-test-name "test"))

(ert-deftest tramp-minio-save-new-object ()
  (tramp-minio-test-save-and-revert "/minio:lab:/bucket/new.txt" ""))

(ert-deftest tramp-minio-error-name-not-code ()
  (cl-letf (((symbol-function 'tramp-minio--check-program) #'ignore)
            ((symbol-function 'tramp-minio--process)
             (lambda (&rest _) '(1 "{\"status\":\"error\",\"code\":\"AccessDenied\",\"message\":\"NoSuchKey.txt\"}" ""))))
    (should (eq (car (should-error (tramp-minio--run "stat" "test"))) 'file-error))))

(ert-deftest tramp-minio-cache-bypass ()
  (let ((tramp-minio--cache (make-hash-table :test 'equal)) (calls 0))
    (tramp-minio--cached 'test 30 (lambda () (cl-incf calls)))
    (let ((remote-file-name-inhibit-cache t))
      (tramp-minio--cached 'test 30 (lambda () (cl-incf calls))))
    (should (= calls 2))))

(provide 'tramp-minio-tests)
