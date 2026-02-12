start_server {tags {"hash-expiry redis8"}} {
    test {HEXPIRE basic usage - set field expiration} {
        r DEL myhash
        r HSET myhash field1 value1 field2 value2
        set result [r HEXPIRE myhash 10 FIELDS 1 field1]
        assert_equal {1} $result
    }

    test {HPEXPIRE basic usage - set field expiration in milliseconds} {
        r DEL myhash
        r HSET myhash field1 value1
        set result [r HPEXPIRE myhash 10000 FIELDS 1 field1]
        assert_equal {1} $result
    }

    test {HEXPIREAT basic usage - set field expiration at timestamp} {
        r DEL myhash
        r HSET myhash field1 value1
        set future_ts [expr {[clock seconds] + 3600}]
        set result [r HEXPIREAT myhash $future_ts FIELDS 1 field1]
        assert_equal {1} $result
    }

    test {HPEXPIREAT basic usage - set field expiration at timestamp in ms} {
        r DEL myhash
        r HSET myhash field1 value1
        set future_ts [expr {[clock milliseconds] + 3600000}]
        set result [r HPEXPIREAT myhash $future_ts FIELDS 1 field1]
        assert_equal {1} $result
    }

    test {HTTL returns field TTL in seconds} {
        r DEL myhash
        r HSET myhash field1 value1
        r HEXPIRE myhash 100 FIELDS 1 field1
        set ttl [r HTTL myhash FIELDS 1 field1]
        assert {[lindex $ttl 0] > 0 && [lindex $ttl 0] <= 100}
    }

    test {HTTL returns -1 for field without expiration} {
        r DEL myhash
        r HSET myhash field1 value1
        set result [r HTTL myhash FIELDS 1 field1]
        assert_equal {-1} $result
    }

    test {HTTL returns -2 for non-existing field} {
        r DEL myhash
        r HSET myhash field1 value1
        set result [r HTTL myhash FIELDS 1 nonexisting]
        assert_equal {-2} $result
    }

    test {HPTTL returns field TTL in milliseconds} {
        r DEL myhash
        r HSET myhash field1 value1
        r HPEXPIRE myhash 100000 FIELDS 1 field1
        set ttl [r HPTTL myhash FIELDS 1 field1]
        assert {[lindex $ttl 0] > 0 && [lindex $ttl 0] <= 100000}
    }

    test {HEXPIRETIME returns absolute expiration timestamp} {
        r DEL myhash
        r HSET myhash field1 value1
        set future_ts [expr {[clock seconds] + 3600}]
        r HEXPIREAT myhash $future_ts FIELDS 1 field1
        set result [r HEXPIRETIME myhash FIELDS 1 field1]
        assert {[lindex $result 0] >= $future_ts - 1 && [lindex $result 0] <= $future_ts + 1}
    }

    test {HPEXPIRETIME returns absolute expiration in milliseconds} {
        r DEL myhash
        r HSET myhash field1 value1
        set future_ts [expr {[clock milliseconds] + 3600000}]
        r HPEXPIREAT myhash $future_ts FIELDS 1 field1
        set result [r HPEXPIRETIME myhash FIELDS 1 field1]
        assert {[lindex $result 0] >= $future_ts - 1000 && [lindex $result 0] <= $future_ts + 1000}
    }

    test {HPERSIST removes field expiration} {
        r DEL myhash
        r HSET myhash field1 value1
        r HEXPIRE myhash 100 FIELDS 1 field1
        set result [r HPERSIST myhash FIELDS 1 field1]
        assert_equal {1} $result
        set ttl [r HTTL myhash FIELDS 1 field1]
        assert_equal {-1} $ttl
    }

    test {Hash field expiration - multiple fields} {
        r DEL myhash
        r HSET myhash f1 v1 f2 v2 f3 v3
        set result [r HEXPIRE myhash 10 FIELDS 3 f1 f2 f3]
        assert_equal {1 1 1} $result
    }

    test {Hash field expiration - mixed existing and non-existing fields} {
        r DEL myhash
        r HSET myhash field1 value1
        set result [r HEXPIRE myhash 10 FIELDS 2 field1 nonexisting]
        assert_equal {1 -2} $result
    }
}

