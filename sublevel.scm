(require-extension utf8)

(module sublevel (sublevel)

(import utf8 scheme chicken)
(use level interfaces srfi-1 srfi-13 lazy-seq irregex)


(define delimiter "\x00") ;; nul character

(define (key->string prefix key)
  (string-join (append prefix (if (list? key) key (list key)))
               delimiter))

;; converts the key part of an operation to a string
(define (convert-key prefix op)
  (if (eq? (length op) 3)
    (list (car op) (key->string prefix (cadr op)) (caddr op))
    (list (car op) (key->string prefix (cadr op)))))

(define resource->prefix car)
(define resource->db cdr)

(define (split-key key)
  (irregex-split delimiter key))

(define (key->list key)
  (if (list? key) key (split-key key)))

(define (remove-prefix prefix key)
  (define (without-prefix a b)
    (if (null? a)
      (string-join b delimiter)
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
  (cond [(and key value)
         (let ([k (key->list (car x))] [v (cadr x)])
           (list (remove-prefix prefix k) v))]
        [key (remove-prefix prefix (key->list x))]
        [value x]))

(define (make-startkey prefix start #!key (reverse #f))
  (if (null? prefix)
    start
    (if start
      (if reverse
        (string-join
          (append prefix (if start (list start) '()))
          delimiter)
        (string-join
          (append prefix (if start (list start) '()))
          delimiter))
      (if reverse
        (string-append
          (string-join prefix delimiter)
          delimiter
          "\xff")
        (string-append
          (string-join prefix delimiter)
          delimiter)))))

(define (make-endkey prefix end #!key (reverse #f))
  (if (null? prefix)
    end
    (if end
      (if reverse
        (string-join
          (append prefix (if end (list end) '()))
          delimiter)
        (string-join
          (append prefix (if end (list end) '()))
          delimiter))
      (if reverse
        (string-append
          (string-join prefix delimiter)
          delimiter)
        (string-append
          (string-join prefix delimiter)
          delimiter
          "\xff")))))

(define sublevel-implementation
  (implementation level-api

    (define (get resource key)
      (db-get (resource->db resource)
              (key->string (resource->prefix resource) key)))

    (define (put resource key value #!key (sync #f))
      (db-put (resource->db resource)
              (key->string (resource->prefix resource) key)
              value
              sync: sync))

    (define (delete resource key #!key (sync #f))
      (db-delete (resource->db resource)
                 (key->string (resource->prefix resource) key)
                 sync: sync))

    (define (batch resource ops #!key (sync #f))
      (db-batch (resource->db resource)
                (map (cut convert-key (resource->prefix resource) <>) ops)
                sync: sync))

    (define (stream resource
                    #!key
                    start
                    end
                    limit
                    reverse
                    (key #t)
                    (value #t)
                    fillcache)
      (let* ([prefix (resource->prefix resource)]
             [start2 (make-startkey prefix start reverse: reverse)]
             [end2 (make-endkey prefix end reverse: reverse)])
        (process-stream key value prefix
          (db-stream (resource->db resource)
                     start: start2
                     end: end2 
                     limit: limit
                     reverse: reverse
                     key: key
                     value: value
                     fillcache: fillcache))))))

(define (sublevel db prefix)
  (make-level sublevel-implementation (cons prefix db))))
