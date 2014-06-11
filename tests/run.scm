(use sublevel level leveldb lazy-seq posix test)

; attempting to open db that doesn't exist
(if (directory? "testdb")
  (delete-directory "testdb" #t))

(define db (sublevel (open-db "testdb") '()))

(test-group "get, put, delete"
  (test "put list key" #t (db-put db '("foo" "bar" "baz") "123"))
  (test "put string key" #t (db-put db "asdf" "456"))
  (test "get list key" "123" (db-get db '("foo" "bar" "baz")))
  (test "get string key" "456" (db-get db "asdf"))
  (test "delete list key" #t (db-delete db '("foo" "bar" "baz")))
  (test "delete string key" #t (db-delete db "asdf")))

(test-group "sublevel using string - get, put, delete"
  (define db2 (sublevel db '("two")))
  (test "sublevel put list key" #t (db-put db2 '("foo" "bar" "baz") "123"))
  (test "sublevel put string key" #t (db-put db2 "asdf" "456"))
  (test "sublevel get list key" "123" (db-get db2 '("foo" "bar" "baz")))
  (test "sublebel get string key" "456" (db-get db2 "asdf"))
  (test "root get list key" "123" (db-get db '("two" "foo" "bar" "baz")))
  (test "root get string key" "456" (db-get db '("two" "asdf")))
  (test "sublevel delete list key" #t (db-delete db2 '("foo" "bar" "baz")))
  (test "sublevel delete string key" #t (db-delete db2 "asdf")))

(test-group "sublevel batches"
  (define db3 (sublevel db '("three")))
  (test "put values using batch" #t
        (db-batch db3 '((put "one" "foo")
                        (put "two" "bar")
                        (delete "two")
                        (put "three" "baz"))))
  (test "get foo back from batch" "foo" (db-get db3 "one"))
  (test-error "do not get bar back from batch" (db-get db3 "two"))
  (test "get baz back from batch" "baz" (db-get db3 "three"))
  (test "get foo back from batch" "foo" (db-get db '("three" "one")))
  (test-error "do not get bar back from batch" (db-get db '("three" "two")))
  (test "get baz back from batch" "baz" (db-get db '("three" "three")))
  (test "put list key values using batch" #t
        (db-batch db3 '((put ("a" "b" "c") "123")
                        (put ("d" "e" "f") "456")
                        (delete ("d" "e" "f")))))
  (test "get (a b c) back from batch" "123" (db-get db3 '("a" "b" "c")))
  (test-error "do not get (d e f) back from batch" (db-get db3 '("d" "e" "f"))))

(test-group "stream keys from a sublevel"
  (define db4 (sublevel db '("four")))
  (db-batch db4 '((put "abc" "123")
                  (put "def" "456")
                  (put "ghi" "789")
                  (put "zzz" "000")))
  (test "get all keys inside prefix"
        '(("abc" "123") ("def" "456") ("ghi" "789") ("zzz" "000"))
        (db-stream db4 lazy-seq->list))
  (test "get limited keys inside prefix"
        '(("abc" "123") ("def" "456"))
        (db-stream db4 lazy-seq->list limit: 2))
  (test "get range of keys inside prefix"
        '(("abc" "123") ("def" "456"))
        (db-stream db4 lazy-seq->list start: "a" end: "g"))
  (test "get all keys outsdie prefix"
        '((("four" "abc") "123")
          (("four" "def") "456")
          (("four" "ghi") "789")
          (("four" "zzz") "000"))
        (db-stream db lazy-seq->list))
  (test "get limited keys outside prefix"
        '((("four" "abc") "123")
          (("four" "def") "456"))
        (db-stream db lazy-seq->list limit: 2))
  (test "get range of keys outside prefix"
        '((("four" "abc") "123")
          (("four" "def") "456"))
        (db-stream db lazy-seq->list start: "a" end: "g"))
  (db-batch db4 '((put ("a" "b" "c") "123")
                  (put ("d" "e" "f") "456")))
  (define db5 (sublevel db '("five")))
  (test "get list keys from stream"
        '((("a" "b" "c") "123")
          (("d" "e" "f") "456"))
        (db-stream db5 lazy-seq->list))
  (test "get key only results from stream"
        '("abc" "def" "ghi" "zzz")
        (db-stream db4 lazy-seq->list key: #t value: #f))
  (test "get key only results from stream - list keys"
        '(("a" "b" "c") ("d" "e" "f"))
        (db-stream db5 lazy-seq->list key: #t value: #f))
  (test "get value only results from stream"
        '("123" "456" "789" "000")
        (db-stream db4 lazy-seq->list key: #f value: #t))
  (test "get value only results from stream - list keys"
        '("123" "456")
        (db-stream db5 lazy-seq->list key: #f value: #t))
  (test "get all keys inside prefix reversed"
        '(("zzz" "000") ("ghi" "789") ("def" "456") ("abc" "123"))
        (db-stream db4 lazy-seq->list reverse: #t)))

(test-exit)
