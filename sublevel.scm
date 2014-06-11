(module sublevel (sublevel)

(import scheme chicken)
(use level interfaces srfi-13)


(define delimiter "\x00") ;; nul character

(define (key->string prefix key)
  (string-join (append prefix (if (list? key) key (list key)))
               delimiter))

(define resource->prefix car)
(define resource->db cdr)


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
      (db-batch (resource->db resource) ops sync: sync))

    (define (stream resource
                    thunk
                    #!key
                    start
                    end
                    limit
                    reverse
                    (key #t)
                    (value #t)
                    fillcache)
      (db-stream (resource->db resource)
                 thunk
                 start: start ;; TODO: add prefix to start/end with tests
                 end: end
                 limit: limit
                 reverse: reverse
                 key: key
                 value: value
                 fillcache: fillcache))))

(define (sublevel db prefix)
  (make-level sublevel-implementation (cons prefix db))))
