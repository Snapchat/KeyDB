if {$::flash_enabled} {
    start_server [list tags {flash} overrides [list storage-provider {flash ./rocks.db} delete-on-evict no storage-flush-period 10]] {

        test { FLASH - GET works after eviction } {
            r set testkey foo
            r flushall cache
            assert_equal {foo} [r get testkey]
        }

        test { DEL of nonexistant key returns 0 } {
            r flushall
            assert_equal {0} [r del foobar]
            assert_equal {0} [r dbsize] "Key count is accurate after non-existant delete"
        }

        test { DEL of flushed key works } {
            r flushall
            r set testkey foo
            assert_equal {1} [r dbsize] "Only one key after first insert"
            r flushall cache
            assert_equal {foo} [r get testkey] "Value still there after flushing cache"
            r del testkey
            assert_equal {0} [r dbsize] "No keys after delete"
        }

    test { SET of existing but flushed key works } {
            r flushall
            r set testkey foo
            assert_equal {1} [r dbsize] "Only one key after first insert"
            r flushall cache
            assert_equal {1} [r dbsize] "Only one key after flushall cache"
            r set testkey bar
            assert_equal {1} [r dbsize] "Only one key after overwrite"
            assert_equal {bar} [r get testkey]
        }

        test { SET of existing but flushed key with EXPIRE works } {
        r flushall
        assert_equal {0} [r dbsize]
        r set testkey foo ex 10000
        assert_equal {1} [r dbsize] "Only one key after first insert"
        r flushall cache
        assert_equal {1} [r dbsize] "Only one key after flushall cache"
        r set testkey bar ex 10000
        assert_equal {1} [r dbsize] "Only one key after overwrite"
        assert_equal {bar} [r get testkey]
        assert [expr [r ttl testkey] > 0]
        }

        test { EXPIRE of existing but flushed key } {
        r flushall
        assert_equal {0} [r dbsize]
        r set testkey foo
        assert_equal {1} [r dbsize]
        r flushall cache
        r expire testkey 10000
        assert_equal {1} [r dbsize]
        assert_equal {foo} [r get testkey]
        assert [expr [r ttl testkey] > 0]
        }

        test { CREATE and UPDATE in transaction, key count is accurate } {
            r flushall
            r multi
            r set testkey 2
            r incr testkey
            r exec
            assert_equal {1} [r dbsize]
            assert_equal {3} [r get testkey]
        }

        test { EXPIRE key count is accurate } {
            r flushall
            r set testkey foo ex 1
            r flushall cache
            assert_equal {1} [r dbsize]
            after 1500
            assert_equal {0} [r dbsize]
        }

        test { SUBKEY EXPIRE persists after cache flush } {
            r flushall
            r sadd testkey foo bar baz
            r expiremember testkey foo 10000
            r flushall cache
            assert [expr [r ttl testkey foo] > 0]
        }

        test { LIST pop works after flushing cache } {
            r flushall
            r lpush testkey foo
            r flushall cache
            assert_equal {foo} [r lpop testkey]
        }

        test { DIGEST string the same after flushing cache } {
            r flushall
            r set testkey foo
            r set testkey1 foo ex 10000
            set expectedDigest [r debug digest]
            r flushall cache
            assert_equal $expectedDigest [r debug digest]
        }

        test { DIGEST list the same after flushing cache } {
            r flushall
            r lpush testkey foo bar
            set expectedDigest [r debug digest]
            r flushall cache
            assert_equal $expectedDigest [r debug digest]
        }

        test { DELETE of flushed set member persists after another flush } {
            r flushall
            r sadd set1 val1 val2 val3
            assert_equal {3} [r scard set1]
            r flushall cache
            r srem set1 val1
            r flushall cache
            assert_equal {2} [r scard set1]
        }

        r flushall
        # If a weak storage memory model is set, wait for any pending snapshot writes to finish
        after 500 
        foreach policy {
            allkeys-random allkeys-lru allkeys-lfu
        } {
            test "FLASH - is eviction working without data loss (successfully stored to flash)? (policy $policy)" {
                # Get the current memory limit and calculate a new limit.
                # Set limit to 100M.
                set used [s used_memory]
                set limit [expr {$used+60*1024*1024}]
                r config set maxmemory $limit
                r config set maxmemory-policy $policy
                # Now add keys equivalent to 1024b until the limit is almost reached.
                set numkeys 0
                r set first val
                while 1 {
                    r set $numkeys xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                    incr numkeys
                    if {[s used_memory]+1024 >= $limit} {
                    break
                    }
                }
                # Add additional keys to force eviction
                # should still be under the limit for maxmemory, however all keys set should still exist between flash and memory
                # check same number of keys exist in addition to values of first and last keys
                set err 0
            set extra_keys [expr floor([expr ($limit * 0.4) / 1024])]
                for {set j 0} {$j < $extra_keys} {incr j} {
                    catch {
                    r set p2$j xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                    } err
                    assert {$err == {OK}}
                }
                if {[log_file_matches [srv 0 stdout] "*Failed to evict*"]} {
                    fail "Server did not evict cleanly (detected full flush)"
                }
                r set last val
                set dbsize [r dbsize]
                assert {[s used_memory] < ($limit*1.2)}
                assert {$dbsize == $numkeys+$extra_keys+2}
                assert {[r get first] == {val}}
                assert {[r get last] == {val}}
                r flushall
            }
        }

    }
}
