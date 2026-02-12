start_server {tags {"redis8"}} {
    test {LMPOP basic usage - pop from LEFT} {
        r DEL mylist1 mylist2
        r RPUSH mylist1 a b c
        r RPUSH mylist2 d e f
        set result [r LMPOP 2 mylist1 mylist2 LEFT COUNT 2]
        assert_equal {mylist1 {a b}} $result
    }

    test {LMPOP basic usage - pop from RIGHT} {
        r DEL mylist1 mylist2
        r RPUSH mylist1 a b c
        set result [r LMPOP 1 mylist1 RIGHT COUNT 1]
        assert_equal {mylist1 c} $result
    }

    test {BLMPOP basic usage} {
        r DEL mylist
        r RPUSH mylist x y z
        set result [r BLMPOP 1 1 mylist LEFT COUNT 1]
        assert_equal {mylist x} $result
    }

    test {ZMPOP basic usage - pop MIN} {
        r DEL myzset1 myzset2
        r ZADD myzset1 1 a 2 b 3 c
        r ZADD myzset2 4 d 5 e 6 f
        set result [r ZMPOP 2 myzset1 myzset2 MIN COUNT 2]
        assert_equal {myzset1 a 1 b 2} $result
    }

    test {ZMPOP basic usage - pop MAX} {
        r DEL myzset
        r ZADD myzset 1 a 2 b 3 c
        set result [r ZMPOP 1 myzset MAX COUNT 1]
        assert_equal {myzset c 3} $result
    }

    test {BZMPOP basic usage} {
        r DEL myzset
        r ZADD myzset 1 x 2 y 3 z
        set result [r BZMPOP 1 1 myzset MIN COUNT 1]
        assert_equal {myzset x 1} $result
    }

    test {SINTERCARD basic usage} {
        r DEL set1 set2 set3
        r SADD set1 a b c d e
        r SADD set2 b c d e f
        r SADD set3 c d e f g
        assert_equal 3 [r SINTERCARD 3 set1 set2 set3]
    }

    test {SINTERCARD with LIMIT} {
        r DEL set1 set2
        r SADD set1 a b c d e
        r SADD set2 a b c d e
        assert_equal 3 [r SINTERCARD 2 set1 set2 LIMIT 3]
    }

    test {EVAL_RO basic usage} {
        r SET mykey "hello"
        set result [r EVAL_RO {return redis.call('GET', KEYS[1])} 1 mykey]
        assert_equal "hello" $result
    }

    test {EVAL_RO is read-only} {
        # EVAL_RO should execute successfully for read operations
        r SET rokey "testvalue"
        set result [r EVAL_RO {return redis.call('GET', KEYS[1])} 1 rokey]
        assert_equal "testvalue" $result
    }

    test {EVALSHA_RO basic usage} {
        set sha [r SCRIPT LOAD {return redis.call('GET', KEYS[1])}]
        r SET testkey "world"
        set result [r EVALSHA_RO $sha 1 testkey]
        assert_equal "world" $result
    }

    test {EXPIRETIME returns absolute expiration timestamp} {
        r DEL mykey
        r SET mykey value
        r EXPIREAT mykey 2000000000
        assert_equal 2000000000 [r EXPIRETIME mykey]
    }

    test {EXPIRETIME returns -1 for key without expiration} {
        r DEL mykey
        r SET mykey value
        assert_equal -1 [r EXPIRETIME mykey]
    }

    test {EXPIRETIME returns -2 for non-existing key} {
        r DEL mykey
        assert_equal -2 [r EXPIRETIME mykey]
    }

    test {PEXPIRETIME returns absolute expiration in milliseconds} {
        r DEL mykey
        r SET mykey value
        r PEXPIREAT mykey 2000000000000
        assert_equal 2000000000000 [r PEXPIRETIME mykey]
    }

    test {BITFIELD_RO basic usage} {
        r DEL mykey
        r SET mykey "\x00\xff"
        set result [r BITFIELD_RO mykey GET u8 0 GET u8 8]
        assert_equal {0 255} $result
    }

    test {LCS basic usage} {
        r SET key1 "ohmytext"
        r SET key2 "mynewtext"
        set result [r LCS key1 key2]
        assert_equal "mytext" $result
    }

    test {LCS with LEN option} {
        r SET key1 "ohmytext"
        r SET key2 "mynewtext"
        set result [r LCS key1 key2 LEN]
        assert_equal 6 $result
    }

    test {LCS with IDX option} {
        r SET key1 "ohmytext"
        r SET key2 "mynewtext"
        set result [r LCS key1 key2 IDX]
        assert_match "*matches*" $result
    }
}

