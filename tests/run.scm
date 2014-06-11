(use sublevel level leveldb posix test)

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

(test-exit)
