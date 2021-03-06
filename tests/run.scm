(use sublevel level leveldb lazy-seq posix test)

; attempting to open db that doesn't exist
(if (directory? "testdb")
  (delete-directory "testdb" #t))

(define db (sublevel (open-db "testdb") '()))

(test-group "get, put, delete"
  (db-put db "asdf" "456")
  (test "get string key after put" "456" (db-get db "asdf"))
  (db-delete db "asdf")
  (test-error "get string key after delete" (db-get db "asdf"))
  (test "get/default after delete"
        "DEFAULT"
        (db-get/default db "asdf" "DEFAULT")))

(test-group "sublevel using string - get, put, delete"
  (define db2 (sublevel db '("two")))
  (db-put db2 "asdf" "456")
  (test "sublevel get string key after put" "456" (db-get db2 "asdf"))
  (test "root get string key" "456" (db-get db '("two" "asdf")))
  (db-delete db2 "asdf")
  (test-error "sublevel get string key after delete" (db-get db2 "asdf"))
  (test-error "root get string key after delete" (db-get db '("two" "asdf"))))

(test-group "sublevel batches"
  (define db3 (sublevel db '("three")))
  (db-batch db3 '((put "one" "foo")
                  (put "two" "bar")
                  (delete "two")
                  (put "three" "baz")))
  (test "get foo back from batch" "foo" (db-get db3 "one"))
  (test-error "do not get bar back from batch" (db-get db3 "two"))
  (test "get baz back from batch" "baz" (db-get db3 "three"))
  (test "get foo back from batch" "foo" (db-get db "three\x00one"))
  (test-error "do not get bar back from batch" (db-get db "three\x00two"))
  (test "get baz back from batch" "baz" (db-get db "three\x00three")))

(test-group "stream keys from a sublevel"
  (define db4 (sublevel db '("four")))
  (db-batch db4 '((put "abc" "123")
                  (put "def" "456")
                  (put "ghi" "789")
                  (put "zzz" "000")))
  (test "get all keys inside prefix"
        '(("abc" . "123") ("def" . "456") ("ghi" . "789") ("zzz" . "000"))
        (lazy-seq->list (db-pairs db4)))
  (test "get limited keys inside prefix"
        '(("abc" . "123") ("def" . "456"))
        (lazy-seq->list (db-pairs db4 limit: 2)))
  (test "get range of keys inside prefix"
        '(("abc" . "123") ("def" . "456"))
        (lazy-seq->list (db-pairs db4 start: "a" end: "g")))
  (test "get all keys outsdie prefix"
        '(("four\x00abc" . "123")
          ("four\x00def" . "456")
          ("four\x00ghi" . "789")
          ("four\x00zzz" . "000")
          ("three\x00one" . "foo")
          ("three\x00three" . "baz"))
        (lazy-seq->list (db-pairs db)))
  (test "get limited keys outside prefix"
        '(("four\x00abc" . "123")
          ("four\x00def" . "456"))
        (lazy-seq->list (db-pairs db limit: 2)))
  (test "get range of keys outside prefix"
        '(("four\x00abc" . "123")
          ("four\x00def" . "456"))
        (lazy-seq->list (db-pairs db start: "four\x00a" end: "four\x00g")))
  (test "get key only results from stream"
        '("abc" "def" "ghi" "zzz")
        (lazy-seq->list (db-keys db4)))
  (test "get value only results from stream"
        '("123" "456" "789" "000")
        (lazy-seq->list (db-values db4)))
  (test "get all keys inside prefix reversed"
        '(("zzz" . "000") ("ghi" . "789") ("def" . "456") ("abc" . "123"))
        (lazy-seq->list (db-pairs db4 reverse: #t)))
  (test "get range of keys inside prefix reversed"
        '(("ghi" . "789") ("def" . "456"))
        (lazy-seq->list (db-pairs db4 reverse: #t start: "gxx" end: "d"))))

(test-group "expand-sublevels"
  (define db1 (sublevel db '("one")))
  (test "expand key for single sublevel"
        '((put "one\x00foo" "123"))
        (expand-sublevels db db1 '((put "foo" "123"))))
  (test "expand multiple keys for single sublevel"
        '((put "one\x00foo" "123")
          (put "one\x00bar" "456")
          (delete "one\x00baz"))
        (expand-sublevels db db1
          '((put "foo" "123")
            (put "bar" "456")
            (delete "baz"))))
  (define db2 (sublevel db1 '("two")))
  (define db3 (sublevel db2 '("three")))
  (test "expand key for multiple sublevels"
        '((put "one\x00two\x00three\x00foo" "123"))
        (expand-sublevels db db3 '((put "foo" "123"))))
  (test "expand multiple keys for multiple sublevels"
        '((put "one\x00two\x00three\x00foo" "123")
          (put "one\x00two\x00three\x00bar" "456")
          (delete "one\x00two\x00three\x00baz"))
        (expand-sublevels db db3
          '((put "foo" "123")
            (put "bar" "456")
            (delete "baz"))))
  (test "expand some but not all sublevels"
        '((put "two\x00three\x00foo" "123")
          (put "two\x00three\x00bar" "456")
          (delete "two\x00three\x00baz"))
        (expand-sublevels db1 db3
          '((put "foo" "123")
            (put "bar" "456")
            (delete "baz"))))
  )

(test-exit)
