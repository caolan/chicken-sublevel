(module sublevel

;; exports
(sublevel
 sublevel-delimiter
 sublevel-split-key
 expand-sublevels
 multilevel-expand
 multilevel-batch)

(import scheme chicken)
(use level interfaces srfi-1 srfi-13 lazy-seq data-structures)


(define sublevel-delimiter "\x00") ;; nul character

(define (key->string prefix key)
  (string-join (append prefix (if (list? key) key (list key)))
               sublevel-delimiter))

;; converts the key part of an operation to a string
(define (convert-key prefix op)
  (if (eq? (length op) 3)
    (list (car op) (key->string prefix (cadr op)) (caddr op))
    (list (car op) (key->string prefix (cadr op)))))

(define (sublevel-split-key key)
  (string-split key sublevel-delimiter))

(define (key->list key)
  (if (list? key) key (sublevel-split-key key)))

(define (remove-prefix prefix key)
  (define (without-prefix a b)
    (if (null? a)
      (string-join b sublevel-delimiter)
      (if (string=? (car a) (car b))
        (without-prefix (cdr a) (cdr b))
        key)))
  (without-prefix prefix (key->list key)))

(define (process-stream key value prefix seq)
  (lazy-map
    (lambda (x)
      (process-stream-item key value prefix x))
    seq))

(define (process-stream-item key value prefix x)
  (cond ((and key value)
         (let ((k (key->list (car x))) (v (cdr x)))
           (cons (remove-prefix prefix k) v)))
        (key (remove-prefix prefix (key->list x)))
        (value x)))

(define (make-startkey prefix start #!key (reverse #f))
  (if (null? prefix)
    start
    (if start
      (if reverse
        (string-join
          (append prefix (if start (list start) '()))
          sublevel-delimiter)
        (string-join
          (append prefix (if start (list start) '()))
          sublevel-delimiter))
      (if reverse
        (string-append
          (string-join prefix sublevel-delimiter)
          sublevel-delimiter
          "\xff")
        (string-append
          (string-join prefix sublevel-delimiter)
          sublevel-delimiter)))))

(define (make-endkey prefix end #!key (reverse #f))
  (if (null? prefix)
    end
    (if end
      (if reverse
        (string-join
          (append prefix (if end (list end) '()))
          sublevel-delimiter)
        (string-join
          (append prefix (if end (list end) '()))
          sublevel-delimiter))
      (if reverse
        (string-append
          (string-join prefix sublevel-delimiter)
          sublevel-delimiter)
        (string-append
          (string-join prefix sublevel-delimiter)
          sublevel-delimiter
          "\xff")))))

(define-record sublevel prefix db)

(define sublevel-implementation
  (implementation level-api

    (define (level-get resource key)
      (db-get (sublevel-db resource)
              (key->string (sublevel-prefix resource) key)))

    (define (level-get/default resource key default)
      (db-get/default (sublevel-db resource)
                      (key->string (sublevel-prefix resource) key)
                      default))

    (define (level-put resource key value #!key (sync #f))
      (db-put (sublevel-db resource)
              (key->string (sublevel-prefix resource) key)
              value
              sync: sync))

    (define (level-delete resource key #!key (sync #f))
      (db-delete (sublevel-db resource)
                 (key->string (sublevel-prefix resource) key)
                 sync: sync))

    (define (level-batch resource ops #!key (sync #f))
      (db-batch (sublevel-db resource)
                (map (cut convert-key (sublevel-prefix resource) <>) ops)
                sync: sync))

    (define (level-stream resource #!key start end limit reverse
                          (key #t) (value #t) fillcache)
      (let* ((prefix (sublevel-prefix resource))
             (start2 (make-startkey prefix start reverse: reverse))
             (end2 (make-endkey prefix end reverse: reverse)))
        (process-stream key value prefix
          (db-stream (sublevel-db resource)
                     start: start2
                     end: end2
                     limit: limit
                     reverse: reverse
                     key: key
                     value: value
                     fillcache: fillcache))))))

(define (sublevel db prefix)
  (make-level 'sublevel
              sublevel-implementation
              (make-sublevel prefix db)))

(define (full-prefix root db)
  (cond ((eq? root db) '())
        ((level? db)
         (full-prefix root (level-resource db)))
        ((sublevel? db)
         (append (full-prefix root (sublevel-db db)) (sublevel-prefix db)))
        (else '())))

(define (expand-sublevels root db ops)
  (let ((prefix (full-prefix root db)))
    (map (lambda (op)
           (let ((type (car op))
                 (key (key->string prefix (cadr op)))
                 (rest (cddr op)))
             (cons type (cons key rest))))
         ops)))

(define-syntax multilevel-expand
  (syntax-rules ()
    ((_ db) '())
    ((_ db (s1 o1) (s2 o2) ...)
     (append
       (expand-sublevels db s1 o1)
       (multilevel-expand db (s2 o2) ...)))))

(define-syntax multilevel-batch
  (syntax-rules ()
    ((_ db) '())
    ((_ db (s1 o1) (s2 o2) ...)
     (db-batch db (multilevel-expand db (s1 o1) (s2 o2) ...)))))

)
