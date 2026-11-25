;;; tramp-minio.el --- MinIO object storage via TRAMP -*- lexical-binding: t; -*-

;; Copyright (C) 2026 Alex Murray <murray.alex@gmail.com>

;; Author: Alex Murray <murray.alex@gmail.com>
;; URL: https://github.com/amurray/tramp-minio
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1"))
;; Keywords: files, comm, convenience
;; SPDX-License-Identifier: GPL-3.0-or-later

;; This file is NOT part of GNU Emacs.

;;; License:

;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; `tramp-minio' adds the TRAMP method `minio' for MinIO and other
;; S3-compatible object storage.  It uses an existing MinIO client
;; (`mc') alias configured outside Emacs.  No credentials are stored in
;; Emacs, and no FUSE mount or remote shell is required.
;;
;; > **NOTE**: This package is experimental.  It is read-only by
;; > default, and enabling writes requires accepting non-atomic upload
;; > semantics.  See "Safety" below.
;;
;; ## Requirements
;;
;; - Emacs 29.1 or later.
;; - The MinIO client (`mc') in `exec-path'.
;; - A POSIX `/bin/sh' for streaming transfers.
;; - An `mc' alias with a bare HTTP(S) endpoint, created outside Emacs:
;;
;;       mc alias set minio https://minio.example.net KEY SECRET
;;
;; ## Installation
;;
;; Install from source with `M-x package-install-file', or add this file
;; to `load-path', then:
;;
;;       (require 'tramp-minio)
;;
;; ## Usage
;;
;; The filename syntax is:
;;
;;       /minio:ALIAS:/BUCKET/KEY
;;
;; Examples:
;;
;;       C-x C-f /minio:minio:/
;;       C-x C-f /minio:minio:/bucket/file.txt
;;       M-x dired RET /minio:minio:/bucket/
;;
;; TAB completes aliases at the host prompt.  Dired lists buckets at the
;; root and objects and common prefixes inside a bucket.  User names,
;; ports, and multi-hop filenames are unsupported.
;;
;; ## Configuration
;;
;; `tramp-minio-allowed-aliases' restricts completion and access to an
;; explicit list of aliases.  `tramp-minio-program' selects the `mc'
;; executable, and `tramp-minio-arguments' supplies trusted global `mc'
;; arguments.  `tramp-minio-metadata-timeout' and
;; `tramp-minio-transfer-timeout' bound subprocesses.
;;
;; `tramp-minio-stat-arguments' defaults to `("--no-list")'; set it to
;; nil for older `mc' clients without `--no-list'.
;;
;; Listings are cached for `tramp-minio-cache-ttl' seconds, and alias
;; completion for `tramp-minio-alias-cache-ttl' seconds.  Use
;; `M-x tramp-minio-clear-cache' or refresh Dired to invalidate them.
;;
;; ## Safety
;;
;; Reads are the default.  Saves, copies to remote locations, and bucket
;; creation require `tramp-minio-read-only' to be nil, and object writes
;; also require `tramp-minio-allow-overwrite'.  Uploads use `mc pipe' and are
;; not atomic or conditional: a new key can be created concurrently, and
;; replacing an object can replace its metadata.  A timed-out upload may
;; still have succeeded.  Revert a buffer after saving before editing it
;; again.  Backups and auto-save are inhibited, and there is no remote
;; locking.
;;
;; Rename, deletion, recursive operations, permission changes, remote
;; processes, and remote Lisp loading are intentionally unsupported.  Use
;; `mc' explicitly for those tasks.  Permissions reported by TRAMP are
;; synthetic; IAM policy is authoritative.  Object identity is synthetic
;; and alias-scoped.  Empty prefixes are not representable, and listings
;; with object/prefix collisions are rejected.  Names containing
;; newlines or repeated slashes are unsupported; `.' and `..' are
;; filesystem navigation components, not literal S3 keys.  Some unusual
;; or quoted filenames may not work.  The client configuration is not
;; pinned transactionally, so do not change aliases during an operation.
;; This package is not a security sandbox; use a restricted `mc'
;; configuration for sensitive deployments.
;;
;; ## Troubleshooting
;;
;; - "MinIO executable not found": set `tramp-minio-program' to an
;;   absolute path or add `mc' to `exec-path'.
;; - "Unknown or disallowed mc alias": check `mc alias list' and
;;   `tramp-minio-allowed-aliases'.
;; - "Invalid MinIO stat schema" or unsupported `--no-list': set
;;   `tramp-minio-stat-arguments' to nil.
;; - "MinIO is read-only": enable writes only after reading the safety
;;   notes above.
;; - "Revert MinIO buffer before another save": revert after a successful
;;   upload, then edit and save again.
;; - "MinIO object changed during download; retry": the object changed
;;   while it was read; retry.
;; - Set `tramp-minio-debug' to log dispatched operation names.  Object
;;   names are not logged.
;;
;; Error output from `mc' is deliberately withheld because it may contain
;; credentials.  Access-denied and missing-object failures are classified
;; when the client supplies structured codes; other failures are reported
;; generically.  JSON schemas and error codes still need verification
;; against the full range of `mc' releases.
;;
;; ## Tests
;;
;; Byte-compile and run the fixture suite:
;;
;;       emacs -Q --batch -L . \
;;         --eval '(setq byte-compile-error-on-warn t)' \
;;         -f batch-byte-compile tramp-minio.el
;;       TRAMP_MINIO_FAKE_MC="$PWD/tests/fake-mc" \
;;         emacs -Q --batch -L . -l tests/tramp-minio-tests.el \
;;         -f ert-run-tests-batch-and-exit
;;
;; The optional live smoke test reads an existing object and performs no
;; writes:
;;
;;       TRAMP_MINIO_TEST_OBJECT=/minio:minio:/bucket/file.txt \
;;         emacs -Q --batch -L . -l tests/tramp-minio-tests.el \
;;         -f ert-run-tests-batch-and-exit
;;
;; ## License
;;
;; GPL-3.0-or-later.  See the COPYING file.

;;; Code:
(require 'tramp)
(require 'json)
(require 'cl-lib)
(require 'subr-x)
(require 'seq)
(require 'ls-lisp)
(require 'filenotify)

(defgroup tramp-minio nil
  "MinIO object storage access through TRAMP."
  :prefix "tramp-minio-"
  :group 'tramp
  :link '(url-link :tag "Homepage" "https://github.com/amurray/tramp-minio")
  :link '(emacs-commentary-link :tag "Commentary" "tramp-minio"))
(defcustom tramp-minio-program "mc"
  "MinIO client executable.
Use an absolute path if `mc' is not available in the variable
`exec-path'."
  :type 'string
  :group 'tramp-minio)

(defcustom tramp-minio-arguments nil
  "Trusted global arguments passed to every `mc' invocation.
These arguments are not a place for per-operation options.  Use absolute
paths for configuration files."
  :type '(repeat string)
  :group 'tramp-minio)

(defcustom tramp-minio-read-only t
  "When non-nil, disable all remote mutations.
Listing, visiting, downloading, and copying out to local storage remain
available."
  :type 'boolean
  :group 'tramp-minio)

(defcustom tramp-minio-allow-overwrite nil
  "When non-nil, permit non-atomic uploads.
Uploads can replace object metadata, and even a new key can be created
concurrently after this package checks for its absence."
  :type 'boolean
  :group 'tramp-minio)

(defcustom tramp-minio-allowed-aliases nil
  "Alias names to allow, or nil for all configured HTTP(S) aliases.
An alias is ignored unless its endpoint is a bare HTTP(S) URL."
  :type '(repeat string)
  :group 'tramp-minio)

(defcustom tramp-minio-metadata-timeout 30
  "Timeout in seconds for metadata commands."
  :type 'number
  :group 'tramp-minio)

(defcustom tramp-minio-transfer-timeout nil
  "Timeout in seconds for transfers, or nil for no limit.
A timed-out transfer may still have completed remotely."
  :type '(choice number (const nil))
  :group 'tramp-minio)

(defcustom tramp-minio-stat-arguments '("--no-list")
  "Arguments passed to `mc stat'.
The default avoids requiring listing permissions.  Set this to nil for
older `mc' releases without `--no-list'."
  :type '(repeat string)
  :group 'tramp-minio)

(defcustom tramp-minio-cache-ttl 3
  "Lifetime in seconds for cached directory listings.
Zero disables listing caching."
  :type 'number
  :group 'tramp-minio)

(defcustom tramp-minio-alias-cache-ttl 30
  "Lifetime in seconds for cached alias completion.
Zero disables alias completion caching."
  :type 'number
  :group 'tramp-minio)
(defvar tramp-minio--cache (make-hash-table :test 'equal))
(defvar tramp-minio--checked-program nil)
(defvar tramp-minio--fresh nil)
(defvar-local tramp-minio--visited-identity nil)
(defvar-local tramp-minio--visited-name nil)
(defvar-local tramp-minio--needs-revert nil)
(dolist (var '(tramp-minio--visited-identity tramp-minio--visited-name tramp-minio--needs-revert))
  (put var 'permanent-local t))
(defcustom tramp-minio-debug nil
  "When non-nil, log dispatched operation names.
Object names and client credentials are not logged."
  :type 'boolean
  :group 'tramp-minio)
(defvar tramp-minio--alias-context nil
  "Cons cell caching aliases during one dispatched operation.")
(define-error 'tramp-minio-missing "MinIO object not found" 'file-missing)

(defun tramp-minio-file-name-p (name)
  "Return non-nil for a MinIO filename or parsed TRAMP structure."
  (and (or (tramp-file-name-p name) (and (stringp name) (tramp-tramp-file-p name)))
       (equal (tramp-file-name-method (tramp-ensure-dissected-file-name name)) "minio")))

(defun tramp-minio--alias-name-p (name)
  "Return non-nil when NAME is a valid mc alias name."
  (and (stringp name) (string-match-p "\\`[A-Za-z][A-Za-z0-9_-]*\\'" name) t))

(defun tramp-minio-clear-cache ()
  "Forget cached MinIO directory listings and aliases."
  (interactive) (clrhash tramp-minio--cache))

(defun tramp-minio--cached (key ttl thunk)
  "Return cached KEY for TTL seconds, or call THUNK."
  (let* ((policy (and (boundp 'remote-file-name-inhibit-cache)
                      remote-file-name-inhibit-cache))
         (key (list tramp-minio-program tramp-minio-arguments tramp-minio-allowed-aliases
                    tramp-minio-read-only key))
         (entry (gethash key tramp-minio--cache)))
    (if (and (not tramp-minio--fresh)
             entry
             (< (- (float-time) (car entry)) ttl)
             (cond ((null policy) t)
                   ((numberp policy) (< (- (float-time) (car entry)) policy))
                   ((consp policy) (time-less-p policy (car entry)))))
        (cdr entry)
      (let ((value (funcall thunk)))
        (when (> ttl 0)
          (when (> (hash-table-count tramp-minio--cache) 512) (tramp-minio-clear-cache))
          (puthash key (cons (float-time) value) tramp-minio--cache)) value))))

(defun tramp-minio--local-directory (&rest _args)
  "Return a verified local temporary directory."
  (unless (and (file-name-absolute-p temporary-file-directory)
               (not (file-remote-p temporary-file-directory)))
    (signal 'file-error '("MinIO requires a local temporary directory")))
  temporary-file-directory)

(defun tramp-minio--temp (&optional suffix directory)
  "Create a private local temporary file (or DIRECTORY) with SUFFIX."
  (let ((default-directory (tramp-minio--local-directory)))
    (make-temp-file (expand-file-name "tramp-minio-" default-directory) directory suffix)))

(defun tramp-minio--process (args timeout &optional raw input-file output-file)
  "Run ARGS locally with TIMEOUT.
RAW returns unibyte output.  INPUT-FILE and OUTPUT-FILE stream standard
input and output through file descriptors instead of Emacs buffers.
Never expose raw client diagnostics, which can contain credentials."
  (let* ((program (let ((default-directory (tramp-minio--local-directory)))
                    (or (executable-find tramp-minio-program)
                        (signal 'file-error '("MinIO executable not found")))))
         (default-directory (file-name-as-directory (tramp-minio--temp nil t)))
         (work default-directory)
         (out (generate-new-buffer " *minio-output*"))
         (err (generate-new-buffer " *minio-error*"))
         (deadline (and timeout (+ (float-time) timeout))) proc)
    (unwind-protect
        (progn
          (with-current-buffer out (set-buffer-multibyte nil))
          (with-current-buffer err (set-buffer-multibyte nil))
          (setq proc (make-process :name "tramp-minio" :buffer out :stderr err
                                    :command (if (or input-file output-file)
                                                (append (list "/bin/sh" "-c"
                                                              "in=$1; out=$2; shift 2; exec < \"$in\"; if [ -n \"$out\" ]; then exec > \"$out\"; fi; exec \"$@\""
                                                              "tramp-minio" (or input-file "/dev/null") (or output-file "") program) args)
                                              (cons program args))
                                   :connection-type 'pipe :coding 'no-conversion
                                   :noquery t :file-handler nil :sentinel #'ignore))
          (when (process-live-p proc) (process-send-eof proc))
          (while (process-live-p proc)
            (when (and deadline (>= (float-time) deadline))
              (signal 'file-error '("MinIO command timed out; remote outcome may be unknown")))
            (accept-process-output proc 0.1))
          (while (accept-process-output proc 0.1))
          (when-let* ((stderr-process (get-buffer-process err)))
            (while (accept-process-output stderr-process 0.1)))
          ;; A missing alias must not cause a successful local write.
          (when (seq-remove (lambda (n) (member n '("." ".."))) (directory-files work nil nil t))
            (signal 'file-error '("Unexpected local output from mc; operation rejected")))
          (list (if (eq (process-status proc) 'exit) (process-exit-status proc) -1)
                (with-current-buffer out (if raw (buffer-string)
                                           (decode-coding-string (buffer-string) 'utf-8-unix)))
                (with-current-buffer err (decode-coding-string (buffer-string) 'utf-8-unix))))
      (when (and proc (process-live-p proc)) (delete-process proc))
      (kill-buffer out) (kill-buffer err)
      (let ((default-directory (tramp-minio--local-directory))) (delete-directory work t)))))

(defun tramp-minio--check-program ()
  "Verify that the configured executable is the MinIO client."
  (unless (equal tramp-minio--checked-program tramp-minio-program)
    (pcase-let ((`(,status ,out ,_) (tramp-minio--process '("--version") tramp-minio-metadata-timeout)))
      (unless (and (zerop status) (string-match-p "\\(?:RELEASE\\.\\|MinIO\\|AIStor\\)" out))
        (signal 'file-error '("Configured mc is not a recognised MinIO client")))
      (setq tramp-minio--checked-program tramp-minio-program))))

(defun tramp-minio--json-lines (text)
  "Parse newline-delimited JSON TEXT."
  (mapcar (lambda (line)
            (let ((row (json-parse-string line :object-type 'alist :array-type 'list)))
              (unless (and (consp row) (consp (car row)) (stringp (alist-get 'status row)))
                (signal 'file-error '("Invalid mc JSON record"))) row))
          (split-string text "\n" t "[ \t\r]+")))

(defun tramp-minio--run (&rest args)
  "Run a structured mc command ARGS; classify only recognised failures."
  (tramp-minio--check-program)
  (pcase-let* ((`(,status ,out ,_err)
                (tramp-minio--process (append '("--json" "--no-color") tramp-minio-arguments args)
                                      tramp-minio-metadata-timeout))
               (rows (condition-case nil (tramp-minio--json-lines out) (error :invalid)))
               (failed (or (not (zerop status)) (eq rows :invalid)
                           (cl-some (lambda (r) (equal (alist-get 'status r) "error")) rows))))
    (when failed
      (let ((codes (unless (eq rows :invalid)
                     (mapcar (lambda (r)
                               (let* ((error-row (alist-get 'error r))
                                      (cause (and (listp error-row) (alist-get 'cause error-row))))
                                 (or (alist-get 'code r)
                                     (and (listp cause)
                                          (or (alist-get 'Code cause)
                                              (alist-get 'code cause)
                                              ;; mc can discard the S3 code when stat fails.
                                              (and (equal (car args) "stat")
                                                   (equal (alist-get 'type error-row) "fatal")
                                                   (equal (assq 'error cause) '(error))
                                                   (equal (alist-get 'message cause) "Object does not exist")
                                                   "NoSuchKey"))))))
                             (cl-remove-if-not (lambda (r) (equal (alist-get 'status r) "error")) rows)))))
        (cond ((member "AccessDenied" codes)
               (signal 'file-error (list "MinIO access denied" (car args))))
              ((and codes (cl-every (lambda (code) (member code '("NoSuchKey" "NoSuchBucket"))) codes))
               (signal 'tramp-minio-missing (list (car args))))
              (t (signal 'file-error (list "MinIO command failed or returned invalid JSON" (car args) status))))))
    rows))

(defun tramp-minio--alias-info ()
  "Return sanitised (ALIAS . URL) pairs, never credentials."
  (delq nil (mapcar (lambda (row)
                     (let ((alias (alist-get 'alias row))
                           (url (or (alist-get 'URL row) (alist-get 'url row))))
                       (when (and (tramp-minio--alias-name-p alias)
                                  (stringp url)
                                  (string-match-p "\\`https?://[^/@?#]+/?\\'" url)
                                  (or (null tramp-minio-allowed-aliases) (member alias tramp-minio-allowed-aliases)))
                         (cons alias url))))
                   (tramp-minio--run "alias" "list"))))

(defun tramp-minio--context-aliases ()
  "Reuse validated aliases within a single dispatched operation."
  (if tramp-minio--alias-context
      (or (car tramp-minio--alias-context)
          (setcar tramp-minio--alias-context (tramp-minio--alias-info)))
    (tramp-minio--alias-info)))

(defun tramp-minio--endpoint (name &optional fresh)
  (let ((host (tramp-file-name-host (tramp-dissect-file-name name))))
    (or (cdr (assoc host (if fresh (tramp-minio--alias-info) (tramp-minio--context-aliases))))
        (signal 'file-error '("MinIO alias unavailable")))))

(defun tramp-minio--aliases (_unused)
  "Return aliases for TRAMP host completion."
  (mapcar (lambda (pair) (list nil (car pair)))
          (tramp-minio--cached 'aliases tramp-minio-alias-cache-ttl #'tramp-minio--alias-info)))

(defun tramp-minio--expand (name &optional directory)
  "Expand filesystem paths, rejecting repeated slashes in MinIO keys."
  (let ((base (or directory default-directory)))
    (when (or (and (tramp-minio-file-name-p name)
                   (string-match-p "//" (tramp-file-name-localname (tramp-dissect-file-name name t))))
              (and (not (file-name-absolute-p name))
                   (tramp-minio-file-name-p base)
                   (string-match-p "//" name)))
      (signal 'file-error '("Repeated slashes in MinIO keys are unsupported"))))
  (tramp-handle-expand-file-name name directory))

(defun tramp-minio--target (name)
  "Translate NAME to an mc target, validating the current alias."
  (with-parsed-tramp-file-name (expand-file-name name) v
    (unless (and (equal v-method "minio") (not v-user) (not (tramp-file-name-port v))
                 (not (tramp-file-name-hop v)) (tramp-minio--alias-name-p v-host)
                 (not (string-match-p "[\n\r\0]\\|//\\|/\\.\\.?\\(?:/\\|\\'\\)" v-localname)))
      (signal 'file-error '("Unsupported MinIO path")))
    (unless (assoc v-host (tramp-minio--context-aliases))
      (signal 'file-error (list "Unknown or disallowed mc alias" v-host)))
    (concat v-host "/" (string-remove-prefix "/" v-localname))))

(defun tramp-minio--attrs (row &optional directory id-format identity)
  "Translate ROW to synthetic file attributes for IDENTITY."
  (let* ((dir (or directory (equal (alist-get 'type row) "folder")))
         (modified (alist-get 'lastModified row))
         (raw-size (alist-get 'size row))
         (time (if (and (stringp modified) (not (string-empty-p modified)))
                   (date-to-time modified)
                 '(0 0)))
         (size (if (and (not dir) (integerp raw-size) (>= raw-size 0)) raw-size 0))
         (id (if (eq id-format 'string) "minio" -1))
         (mode (if tramp-minio-read-only (if dir "dr-x------" "-r--------")
                 (if dir "drwx------" "-rw-------"))))
    (list (and dir t) 1 id id time time time size
          mode nil (sxhash-equal identity) (sxhash-equal (and identity (file-remote-p identity))))))

(defun tramp-minio--entries (directory &optional id-format)
  "Return (NAME . ATTRIBUTES) pairs; reject object/prefix collisions."
  (let* ((directory (file-name-as-directory (expand-file-name directory)))
         (target (tramp-minio--target directory))
         (root (and (string-match-p "\\`[^/]+/\\'" target) t))
         (rows (tramp-minio--cached (list 'ls (tramp-minio--endpoint directory) target) tramp-minio-cache-ttl
                                   (lambda () (tramp-minio--run "ls" target))))
         (seen (make-hash-table :test 'equal)) entries)
    (dolist (row rows)
      (let* ((key (alist-get 'key row)) (name (and (stringp key) (string-remove-suffix "/" key))))
        (unless (and name (not (member name '("" "." ".."))) (not (string-match-p "[/\n\r\0]" name)))
          (signal 'file-error '("Unrepresentable MinIO listing entry")))
        (when (gethash name seen) (signal 'file-error (list "Ambiguous MinIO object/prefix" name)))
        (puthash name t seen)
        (push (cons name (tramp-minio--attrs row root id-format (concat directory name))) entries)))
    (nreverse entries)))

(defun tramp-minio--stat (name)
  "Fetch fresh metadata for NAME, returning nil only on a recognised absence."
  (condition-case nil
      (let ((rows (apply #'tramp-minio--run "stat" (append tramp-minio-stat-arguments (list (tramp-minio--target name))))))
        (cond ((zerop (length rows))
               (signal 'file-error '("Ambiguous or empty mc stat response")))
              ((> (length rows) 1)
               (if (member "--no-list" tramp-minio-stat-arguments)
                   (signal 'file-error '("Ambiguous or empty mc stat response"))
                 nil))
              (t (let ((row (car rows)))
                   (unless (and (equal (alist-get 'status row) "success")
                                (or (equal (alist-get 'type row) "folder")
                                    (and (integerp (alist-get 'size row)) (>= (alist-get 'size row) 0)
                                         (stringp (alist-get 'lastModified row))
                                         (or (stringp (alist-get 'etag row)) (stringp (alist-get 'versionID row))))))
                     (signal 'file-error '("Invalid MinIO stat schema")))
                   row))))
    (tramp-minio-missing nil)))

(defun tramp-minio--parent-attributes (name id-format)
  "Return listed directory attributes for NAME from its parent.
ID-FORMAT selects numeric or string identifiers."
  (condition-case nil
      (let* ((bare (directory-file-name name))
             (attrs (cdr (assoc (file-name-nondirectory bare)
                                (tramp-minio--entries (file-name-directory bare) id-format)))))
        (and (car attrs) attrs))
    (tramp-minio-missing nil)))

(defun tramp-minio--file-attributes (name &optional id-format)
  "Return synthetic attributes for NAME."
  (let* ((name (expand-file-name name)) (target (tramp-minio--target name)))
    (cond ((string-match-p "\\`[^/]+/\\'" target) (tramp-minio--attrs nil t id-format name))
          ;; Explicit directory paths and bucket roots use parent listings.
          ((or (string-suffix-p "/" name) (string-match-p "\\`[^/]+/[^/]+\\'" target))
           (tramp-minio--parent-attributes name id-format))
          (t (let ((row (tramp-minio--stat name)))
               (if (and row (not (equal (alist-get 'type row) "folder")))
                   (tramp-minio--attrs row nil id-format name)
                 (tramp-minio--parent-attributes name id-format)))))))

(defun tramp-minio--exists (name) (and (tramp-minio--file-attributes name) t))
(defun tramp-minio--directory-p (name) (eq t (car (tramp-minio--file-attributes name))))
(defun tramp-minio--regular-p (name) (let ((a (tramp-minio--file-attributes name))) (and a (not (car a)))))
(defun tramp-minio--writable-p (name)
  (let* ((expanded (expand-file-name name))
         (target (tramp-minio--target expanded)))
    (and (not tramp-minio-read-only) tramp-minio-allow-overwrite
         (or (tramp-minio--exists expanded)
             (tramp-minio--directory-p (file-name-directory expanded))
             ;; Virtual prefixes need not pre-exist for object keys; check the bucket.
             (and (string-match "\\`[^/]+/\\([^/]+\\)/.+" target)
                  (not (string-suffix-p "/" target))
                  (tramp-minio--directory-p
                   (concat (file-remote-p expanded) "/" (match-string 1 target) "/")))))))
(defun tramp-minio--modes (name &optional _flag)
  (let ((a (tramp-minio--file-attributes name)))
    (when a (if (car a) (if tramp-minio-read-only #o500 #o700) (if tramp-minio-read-only #o400 #o600)))))

(defun tramp-minio--filter-directory-items (items directory key-fn full match nosort count)
  "Filter, sort, truncate, and optionally qualify directory ITEMS."
  (let ((case-fold-search nil)
        (dir (file-name-as-directory (expand-file-name directory))))
    (when match (setq items (cl-remove-if-not (lambda (x) (string-match-p match (funcall key-fn x))) items)))
    (unless nosort (setq items (sort items (lambda (a b) (string-lessp (funcall key-fn a) (funcall key-fn b))))))
    (when count (setq items (seq-take items count)))
    (if full
        (mapcar (lambda (x)
                  (if (consp x) (cons (concat dir (car x)) (cdr x)) (concat dir x)))
                items)
      items)))

(defun tramp-minio--directory-files (directory &optional full match nosort count)
  (let ((names (append '("." "..") (mapcar #'car (tramp-minio--entries directory)))))
    (tramp-minio--filter-directory-items names directory #'identity full match nosort count)))

(defun tramp-minio--directory-files-and-attributes (directory &optional full match nosort id-format count)
  (let* ((dir (file-name-as-directory (expand-file-name directory)))
         (bare (directory-file-name dir))
         (parent (directory-file-name (file-name-directory bare)))
         (entries (append (list (cons "." (tramp-minio--attrs nil t id-format bare))
                                (cons ".." (tramp-minio--attrs nil t id-format parent)))
                          (tramp-minio--entries dir id-format))))
    (tramp-minio--filter-directory-items entries dir #'car full match nosort count)))

(defun tramp-minio--file-name-all-completions (filename directory)
  "Complete FILENAME in DIRECTORY, adding a trailing slash to directories."
  (let ((completion-ignore-case nil)
        (entries (tramp-minio--entries directory)))
    (all-completions
     filename
     (append '("./" "../")
             (mapcar (lambda (entry)
                       (if (eq t (cadr entry))
                           (concat (car entry) "/")
                         (car entry)))
                     entries)))))

(defun tramp-minio--object-target (name)
  "Require an unambiguous object target, not a bucket or prefix."
  (let ((target (tramp-minio--target name)))
    (unless (and (string-match-p "\\`[^/]+/[^/]+/.+" target) (not (string-suffix-p "/" target)))
      (signal 'file-error '("Expected a MinIO object"))) target))

(defun tramp-minio--identity (row &optional name)
  "Return opaque object identity from stat ROW."
  (when row (list (alist-get 'versionID row) (alist-get 'etag row)
                  (alist-get 'lastModified row) (alist-get 'size row)
                  (and name (tramp-minio--endpoint name)) name)))

(defun tramp-minio--missing-identity (name)
  "Return the identity of absent NAME."
  (list 'missing (tramp-minio--endpoint name) name))

(defun tramp-minio--suffix (name)
  "Return a safe temporary filename suffix preserving coding hints."
  (let ((base (file-name-nondirectory name)))
    (when (and (string-match "\\(\\.[A-Za-z0-9._-]+\\)\\'" base) (< (length (match-string 1 base)) 80))
      (match-string 1 base))))

(defun tramp-minio--download (name)
  "Download NAME and return (TEMPFILE . IDENTITY), checking for concurrent changes."
  (let* ((target (tramp-minio--object-target name))
         (before (tramp-minio--stat name)))
    (unless before (signal 'file-missing (list "No such MinIO object" name)))
    (when (equal (alist-get 'type before) "folder") (signal 'file-error '("Cannot download prefix")))
    (let ((tmp (tramp-minio--temp (tramp-minio--suffix name))) success)
      (unwind-protect
          (progn
            ;; cat emits raw bytes, never JSON.  Version IDs pin versioned reads.
            (pcase-let ((`(,status ,data ,_)
                         (tramp-minio--process
                          (append '("--no-color") tramp-minio-arguments '("cat")
                                  (let ((id (alist-get 'versionID before)))
                                    (when (and (stringp id) (not (member id '("" "null")))) (list "--version-id" id)))
                                  (list target)) tramp-minio-transfer-timeout t nil tmp)))
              (unless (and (zerop status) (equal (file-attribute-size (file-attributes tmp)) (alist-get 'size before)))
                (signal 'file-error '("MinIO download failed or size changed")))
              (ignore data))
            (unless (equal (tramp-minio--endpoint name) (tramp-minio--endpoint name t))
              (signal 'file-error '("MinIO alias changed during download")))
            (unless (equal (tramp-minio--identity before name) (tramp-minio--identity (tramp-minio--stat name) name))
              (signal 'file-error '("MinIO object changed during download; retry")))
            (setq success t) (cons tmp (tramp-minio--identity before name)))
        (unless success (delete-file tmp))))))

(defun tramp-minio--local-copy (name) (car (tramp-minio--download name)))

(defun tramp-minio--insert (name &optional visit beg end replace)
  "Insert remote contents using a private local staging file."
  (setq name (expand-file-name name))
  (when (and visit (or beg end)) (signal 'file-error '("Partial visiting is unsupported")))
  (let* ((download (condition-case err (tramp-minio--download name)
                     (file-missing
                      (when visit
                        (setq buffer-file-name name
                              tramp-minio--visited-name name
                              tramp-minio--visited-identity (tramp-minio--missing-identity name)
                              tramp-minio--needs-revert nil))
                      (signal (car err) (cdr err)))))
         (tmp (car download)))
    (unwind-protect
        (let* ((coding-system-for-read (or coding-system-for-read
                                           (car (find-operation-coding-system 'insert-file-contents name))))
               (result (insert-file-contents tmp nil beg end replace)))
          (when visit
            (setq buffer-file-name name tramp-minio--visited-name name
                  tramp-minio--visited-identity (cdr download) tramp-minio--needs-revert nil)
            ;; Emacs can return an integer timestamp from date-to-time,
            ;; but set-visited-file-modtime reserves integers for flags.
            (let ((modified (nth 2 (cdr download))))
              (set-visited-file-modtime
               (and modified (time-convert (date-to-time modified) 'list))))
            (set-buffer-modified-p nil)
            ;; Do not create remote backups, locks or local autosave copies implicitly.
            (tramp-minio--inhibit-buffer-conveniences))
          (list name (cadr result)))
      (delete-file tmp))))

(defun tramp-minio--verify (&optional buffer)
  "Compare BUFFER's recorded identity with fresh object metadata."
  (with-current-buffer (or buffer (current-buffer))
    (and (not tramp-minio--needs-revert) (equal buffer-file-name tramp-minio--visited-name)
         (equal tramp-minio--visited-identity
                (let ((row (tramp-minio--stat buffer-file-name)))
                  (if row (tramp-minio--identity row buffer-file-name)
                    (tramp-minio--missing-identity buffer-file-name)))))))

(defun tramp-minio--mutation ()
  "Reject mutations unless explicitly enabled."
  (when tramp-minio-read-only (signal 'file-error '("MinIO is read-only"))))

(defun tramp-minio--upload (name tmp allow-existing &optional expected)
  "Upload TMP to NAME; require opt-in to non-atomic overwrite semantics."
  (tramp-minio--mutation)
  (let* ((tramp-minio--fresh t) (target (tramp-minio--object-target name))
         (old (tramp-minio--stat name))
         (parent (file-name-directory name))
         (entry (condition-case nil (assoc (file-name-nondirectory name) (tramp-minio--entries parent)) (tramp-minio-missing nil))))
    (when (or (cadr entry) (equal (alist-get 'type old) "folder"))
      (signal 'file-error '("Cannot upload to a MinIO prefix")))
    (when (and expected (not (equal expected (if old (tramp-minio--identity old name)
                                               (tramp-minio--missing-identity name)))))
      (signal 'file-error '("MinIO object changed since visiting")))
    (when old
      (unless (and allow-existing tramp-minio-allow-overwrite)
        (signal 'file-already-exists (list "MinIO overwrite not enabled" name))))
    ;; mc pipe cannot conditionally create or replace objects.  Even a new
    ;; name can be concurrently created, so require the explicit opt-in.
    (unless tramp-minio-allow-overwrite
      (signal 'file-error '("mc cannot guarantee exclusive creation; enable tramp-minio-allow-overwrite to accept the race")))
    (unless (equal (tramp-minio--endpoint name) (tramp-minio--endpoint name t))
      (signal 'file-error '("MinIO alias changed before upload")))
    (let ((size (file-attribute-size (file-attributes tmp))))
      (unwind-protect
          (pcase-let ((`(,status ,out ,_)
                       (tramp-minio--process (append '("--json" "--no-color") tramp-minio-arguments (list "pipe" target))
                                             tramp-minio-transfer-timeout nil tmp)))
            ;; Some mc releases prefix pipe's JSON with carriage-return
            ;; progress frames even in JSON mode.
            (when (string-match "\\`\r[^\n{]*{" out)
              (setq out (substring out (1- (match-end 0)))))
            (let* ((rows (and (zerop status)
                              (condition-case nil (tramp-minio--json-lines out)
                                ((json-error file-error) nil))))
                   (row (car rows)))
              (unless (and (= (length rows) 1)
                           (equal (alist-get 'status row) "success")
                           (equal (alist-get 'target row) target)
                           (equal (alist-get 'size row) size))
                (signal 'file-error '("MinIO upload failed; remote outcome may be unknown")))))
        (tramp-minio-clear-cache)))
    ;; No server response binding the bytes to a new ETag is available here.
    ;; Do not claim a later stat represents our upload.
    nil))

(defun tramp-minio--write (start end name &optional append visit _lockname mustbenew)
  "Write a complete object; refuse append and atomic exclusive creation."
  (tramp-minio--mutation)
  (setq name (expand-file-name name))
  (when (and tramp-minio--needs-revert (equal name buffer-file-name))
    (signal 'file-error '("Revert MinIO buffer before another save")))
  (when (or append (eq mustbenew 'excl)) (signal 'file-error '("MinIO append/exclusive creation unsupported")))
  (let ((tmp (tramp-minio--temp (tramp-minio--suffix name)))
        (allow-existing (not mustbenew)))
    (unwind-protect
        (let ((coding-system-for-write (or coding-system-for-write
                                           (cdr (find-operation-coding-system 'write-region start end name)))))
          (when (and mustbenew (tramp-minio--stat name))
            (unless tramp-minio-allow-overwrite
              (signal 'file-already-exists (list "MinIO overwrite not enabled" name)))
            (unless (yes-or-no-p (format "Overwrite %s? " name))
              (signal 'file-already-exists (list "File already exists" name)))
            (setq allow-existing t))
          (write-region start end tmp nil 'silent)
          (tramp-minio--upload name tmp allow-existing
                               (and (equal name tramp-minio--visited-name) tramp-minio--visited-identity))
          (cond ((or (eq visit t) (stringp visit))
                 (let ((visited (if (stringp visit) (expand-file-name visit) name)))
                   (setq buffer-file-name visited
                         tramp-minio--visited-name nil tramp-minio--visited-identity nil
                         tramp-minio--needs-revert (and (tramp-minio-file-name-p visited) t))
                   (set-visited-file-modtime '(0 0)) (set-buffer-modified-p nil)
                   (when (tramp-minio-file-name-p visited)
                     (setq-local file-precious-flag nil)
                     (tramp-minio--inhibit-buffer-conveniences)
                     (message "Uploaded; revert before editing again to establish a fresh object identity"))))
                ((equal name buffer-file-name)
                 (setq tramp-minio--visited-name nil tramp-minio--visited-identity nil
                       tramp-minio--needs-revert t))))
      (delete-file tmp))))

(defun tramp-minio--copy (source destination &optional ok keep-time preserve-uid preserve-extended-attributes)
  "Copy SOURCE to DESTINATION using a local staging file."
  (setq source (expand-file-name source) destination (expand-file-name destination))
  (when (or preserve-uid preserve-extended-attributes) (tramp-minio--unsupported))
  (let ((src (tramp-minio-file-name-p source)) (dst (tramp-minio-file-name-p destination)))
    (when (or (and (file-remote-p source) (not src)) (and (file-remote-p destination) (not dst))) (tramp-minio--unsupported))
    (when dst (tramp-minio--mutation))
    (when (file-directory-p destination)
      (setq destination (expand-file-name (file-name-nondirectory source) (file-name-as-directory destination))))
    (when (equal source destination)
      (signal 'file-error (list "Copying file to itself" source)))
    (when (or (file-directory-p source) (and dst keep-time)) (tramp-minio--unsupported))
    (when (and (file-exists-p destination) (null ok))
      (signal 'file-already-exists (list "File already exists" destination)))
    (when (and dst (not tramp-minio-allow-overwrite))
      (signal 'file-error '("mc cannot guarantee exclusive creation; enable tramp-minio-allow-overwrite to accept the race")))
    (when (and (file-exists-p destination) (integerp ok))
      (unless (yes-or-no-p (format "Overwrite %s? " destination))
        (signal 'file-already-exists (list "File already exists" destination)))
      (setq ok t))
    (let* ((download (when src (tramp-minio--download source)))
           (tmp (if src (car download) source)))
      (unwind-protect
          (if dst (tramp-minio--upload destination tmp (not (null ok)))
            (progn (copy-file tmp destination ok)
                   (when keep-time
                     (let ((mtime (if src
                                      (and (nth 2 (cdr download)) (date-to-time (nth 2 (cdr download))))
                                    (file-attribute-modification-time (file-attributes source)))))
                       (when mtime (set-file-times destination mtime))))))
        (when src (delete-file tmp))))))

(defun tramp-minio--delete (&rest _args)
  "Reject deletion: mc cannot conditionally delete the observed object."
  (tramp-minio--unsupported))
(defun tramp-minio--rename (&rest _args)
  "Reject non-atomic rename to eliminate source deletion risks."
  (tramp-minio--unsupported))
(defun tramp-minio--mkdir (name &optional parents)
  "Create a bucket only; virtual prefixes are not directories."
  (tramp-minio--mutation)
  (let ((target (string-remove-suffix "/" (tramp-minio--target name))))
    (unless (string-match-p "\\`[^/]+/[^/]+\\'" target) (tramp-minio--unsupported))
    (unless (and parents (tramp-minio--directory-p name))
      (unwind-protect (tramp-minio--run "mb" target) (tramp-minio-clear-cache))))
  nil)

(defun tramp-minio--access (name string)
  "Signal an error if NAME is not visible. STRING is the error message."
  (unless (tramp-minio--exists name) (signal 'file-error (list string name))))

(defun tramp-minio--dired-uncache (&rest _args) (tramp-minio-clear-cache))

(defun tramp-minio--insert-directory (file switches &optional wildcard full-directory-p)
  (defvar ls-lisp-dirs-first)
  (defvar ls-lisp-emulation)
  (defvar ls-lisp-ignore-case)
  (defvar ls-lisp-use-insert-directory-program)
  (defvar ls-lisp-verbosity)
  (unless switches (setq switches ""))
  (when (and (directory-name-p file) (not full-directory-p))
    (setq switches (concat switches " -F")))
  (let (ls-lisp-emulation ls-lisp-ignore-case ls-lisp-dirs-first
        ls-lisp-verbosity ls-lisp-use-insert-directory-program
        (listing-start (point)))
    (when (fboundp 'ls-lisp-set-options) (ls-lisp-set-options))
    (tramp-run-real-handler 'insert-directory
                            (list file switches wildcard full-directory-p))
    (unless (seq-some (lambda (option)
                        (and (not (string-prefix-p "--" option))
                             (string-search "l" option)))
                      (split-string switches))
      (save-excursion
        (save-restriction
          (narrow-to-region listing-start (point))
          (goto-char (point-min))
          (while (< (point) (point-max))
            (let* ((end (line-end-position))
                   (start (text-property-not-all (point) end 'dired-filename t)))
              (when start
                (delete-region start (or (text-property-any start end 'dired-filename t)
                                         end))))
            (if (eolp)
                (delete-region (point) (progn (forward-line 1) (point)))
              (forward-line 1))))))))

(defun tramp-minio--truename (name &rest _args) (expand-file-name name))

(defvar tramp-minio--operation nil
  "File operation currently dispatched by the MinIO handler.")

(defun tramp-minio--unsupported (&rest _args)
  (signal 'file-error
          (list "Operation not supported by MinIO object storage"
                (or tramp-minio--operation 'unknown))))

(defun tramp-minio--no-watch (&rest _args)
  "Report unavailable notification support using the expected condition."
  (signal 'file-notify-error '("MinIO does not support file watches")))

(defconst tramp-minio-file-name-handler-alist
  '((file-attributes . tramp-minio--file-attributes)
    (file-exists-p . tramp-minio--exists)
    (file-directory-p . tramp-minio--directory-p)
    (file-accessible-directory-p . tramp-minio--directory-p)
    (file-regular-p . tramp-minio--regular-p)
    (file-readable-p . tramp-minio--exists)
    (file-writable-p . tramp-minio--writable-p)
    (file-modes . tramp-minio--modes)
    (directory-files . tramp-minio--directory-files)
    (directory-files-and-attributes . tramp-minio--directory-files-and-attributes)
    (file-local-copy . tramp-minio--local-copy)
    (insert-file-contents . tramp-minio--insert)
    (write-region . tramp-minio--write)
    (copy-file . tramp-minio--copy)
    (rename-file . tramp-minio--rename)
    (delete-file . tramp-minio--delete)
    (make-directory . tramp-minio--mkdir)
    (insert-directory . tramp-minio--insert-directory)
    (expand-file-name . tramp-minio--expand)
    (file-remote-p . tramp-handle-file-remote-p)
    (file-name-directory . tramp-handle-file-name-directory)
    (file-name-nondirectory . tramp-handle-file-name-nondirectory)
    (directory-file-name . tramp-handle-directory-file-name)
    (file-name-as-directory . tramp-handle-file-name-as-directory)
    (file-name-all-completions . tramp-minio--file-name-all-completions)
    (file-name-completion . tramp-handle-file-name-completion)
    (file-truename . tramp-minio--truename)
    (file-newer-than-file-p . tramp-handle-file-newer-than-file-p)
    (set-visited-file-modtime . tramp-handle-set-visited-file-modtime)
    (verify-visited-file-modtime . tramp-minio--verify)
    (substitute-in-file-name . tramp-handle-substitute-in-file-name)
    (file-symlink-p . ignore) (file-executable-p . ignore)
    (file-locked-p . ignore) (lock-file . ignore) (unlock-file . ignore)
    (vc-registered . ignore) (file-ownership-preserved-p . ignore)
    (set-file-modes . tramp-minio--unsupported) (set-file-times . tramp-minio--unsupported)
    (delete-directory . tramp-minio--unsupported)
    (copy-directory . tramp-minio--unsupported)
    (make-symbolic-link . tramp-minio--unsupported)
    (add-name-to-file . tramp-minio--unsupported)
    (process-file . tramp-minio--unsupported)
    (start-file-process . tramp-minio--unsupported)
    (make-process . tramp-minio--unsupported)
    (shell-command . tramp-minio--unsupported)
    (access-file . tramp-minio--access)
    (file-equal-p . tramp-handle-file-equal-p)
    (file-in-directory-p . tramp-handle-file-in-directory-p)
    (file-name-case-insensitive-p . ignore)
    (file-notify-add-watch . tramp-minio--no-watch)
    (file-notify-rm-watch . ignore) (file-notify-valid-p . ignore)
    (file-selinux-context . ignore) (file-extended-attributes . ignore)
    (tramp-get-home-directory . ignore)
    (tramp-get-remote-uid . ignore) (tramp-get-remote-gid . ignore)
    (tramp-get-remote-groups . ignore)
    (list-system-processes . ignore)
    (file-system-info . ignore) (file-acl . ignore)
    (set-file-acl . tramp-minio--unsupported)
    (make-nearby-temp-file . tramp-minio--unsupported)
    (temporary-file-directory . tramp-minio--local-directory)
    (dired-uncache . tramp-minio--dired-uncache)
    (load . tramp-minio--unsupported)
    (make-auto-save-file-name . tramp-handle-make-auto-save-file-name)
    (find-backup-file-name . tramp-handle-find-backup-file-name)
    (file-user-uid . tramp-minio--uid)
    (file-user-gid . tramp-minio--uid)
    (memory-info . ignore) (process-attributes . ignore)
    (set-file-selinux-context . tramp-minio--unsupported)
    (set-file-extended-attributes . tramp-minio--unsupported)
    (tramp-set-file-uid-gid . tramp-minio--unsupported)
    (exec-path . ignore) (unhandled-file-name-directory . tramp-minio--local-directory)))

(defun tramp-minio--uid (&rest _) -1)

(defun tramp-minio--inhibit-buffer-conveniences ()
  "Disable local backup and auto-save behaviour for a MinIO buffer."
  (setq-local backup-inhibited t)
  (setq-local auto-save-default nil)
  (auto-save-mode -1))

(defun tramp-minio--visit-policy ()
  "Apply storage-specific save policy after major mode initialisation."
  (when (and buffer-file-name (tramp-minio-file-name-p buffer-file-name))
    (unless (equal buffer-file-name tramp-minio--visited-name)
      (setq tramp-minio--visited-identity nil tramp-minio--visited-name nil))
    (setq-local file-precious-flag nil)
    (tramp-minio--inhibit-buffer-conveniences)))
(add-hook 'find-file-hook #'tramp-minio--visit-policy)
(add-hook 'after-change-major-mode-hook #'tramp-minio--visit-policy)
(add-hook 'after-set-visited-file-name-hook #'tramp-minio--visit-policy)

(defun tramp-minio-file-name-handler (operation &rest args)
  (when tramp-minio-debug (message "tramp-minio: %S" operation))
  (let ((tramp-minio--operation operation)
        ;; Share the alias cache with a nested dispatch, otherwise start fresh.
        (tramp-minio--alias-context (or tramp-minio--alias-context (list nil))))
    (save-match-data
      (if-let* ((handler (cdr (assq operation tramp-minio-file-name-handler-alist))))
          (apply handler args)
        ;; These primitives manipulate names or inspect Emacs buffers, not storage.
        (if (memq operation '(abbreviate-file-name byte-compiler-base-file-name
                              get-file-buffer file-name-absolute-p file-relative-name
                              file-name-sans-versions file-name-unquote file-name-quote
                              directory-name-p make-lock-file-name))
            (tramp-run-real-handler operation args)
          (tramp-minio--unsupported))))))

(add-to-list 'tramp-methods '("minio"))
(add-to-list 'tramp-default-host-alist '("minio" nil ""))
(add-to-list 'tramp-default-user-alist '("minio" nil nil))
(tramp-register-foreign-file-name-handler #'tramp-minio-file-name-p #'tramp-minio-file-name-handler)
(tramp-set-completion-function "minio" '((tramp-minio--aliases "")))

(defun tramp-minio-unload-function ()
  "Remove MinIO registrations and cached metadata."
  (remove-hook 'find-file-hook #'tramp-minio--visit-policy)
  (remove-hook 'after-change-major-mode-hook #'tramp-minio--visit-policy)
  (remove-hook 'after-set-visited-file-name-hook #'tramp-minio--visit-policy)
  (tramp-minio-clear-cache)
  (setq tramp-methods (assoc-delete-all "minio" tramp-methods)
        tramp-default-host-alist (assoc-delete-all "minio" tramp-default-host-alist)
        tramp-default-user-alist (assoc-delete-all "minio" tramp-default-user-alist)
        tramp-foreign-file-name-handler-alist
        (assq-delete-all 'tramp-minio-file-name-p tramp-foreign-file-name-handler-alist))
  (tramp-set-completion-function "minio" nil)
  nil)

(provide 'tramp-minio)
;;; tramp-minio.el ends here
