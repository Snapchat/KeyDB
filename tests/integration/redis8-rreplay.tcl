# Redis 8 Commands - RREPLAY Active-Active Replication Tests
# Tests all new Redis 8 commands for active-active replication compatibility

start_server {tags {"replication"}} {
start_server {} {
    set master [srv -1 client]
    set master_host [srv -1 host]
    set master_port [srv -1 port]
    set replica [srv 0 client]

    # Setup active-active replication
    test {Setup active-active replication for Redis 8 commands} {
        $replica replicaof $master_host $master_port
        $replica config set active-replica yes
        wait_for_sync $replica
    }

    # Test LMPOP replication
    test {LMPOP replicates correctly via RREPLAY} {
        $master del mylist
        $master rpush mylist a b c d e
        set result [$master lmpop 1 mylist LEFT COUNT 2]
        wait_for_ofs_sync $master $replica
        assert_equal [$replica llen mylist] 3
        assert_equal [$replica lrange mylist 0 -1] {c d e}
    }

    # Test ZMPOP replication
    test {ZMPOP replicates correctly via RREPLAY} {
        $master del myzset
        $master zadd myzset 1 a 2 b 3 c 4 d
        set result [$master zmpop 1 myzset MIN COUNT 2]
        wait_for_ofs_sync $master $replica
        assert_equal [$replica zcard myzset] 2
        assert_equal [$replica zrange myzset 0 -1] {c d}
    }

    # Test hash field expiry replication
    test {HEXPIRE replicates correctly via RREPLAY} {
        $master del myhash
        $master hset myhash field1 value1 field2 value2
        $master hexpire myhash 100 FIELDS 1 field1
        wait_for_ofs_sync $master $replica
        
        # Verify expiry was replicated
        set ttl [$replica httl myhash FIELDS 1 field1]
        assert {[lindex $ttl 0] > 0 && [lindex $ttl 0] <= 100}
    }

    # Test FUNCTION LOAD replication
    test {FUNCTION LOAD replicates correctly via RREPLAY} {
        $master function flush
        set code {#!lua name=testlib
redis.register_function('testfunc', function(keys, args)
    return 'hello'
end)}
        $master function load $code
        wait_for_ofs_sync $master $replica
        
        # Verify function was replicated
        set libs [$replica function list]
        assert_match "*testlib*" $libs
    }

    # Test FCALL replication (with writes)
    test {FCALL with writes replicates correctly via RREPLAY} {
        $master function flush
        set code {#!lua name=writelib
redis.register_function('writefunc', function(keys, args)
    redis.call('SET', keys[1], args[1])
    return 'OK'
end)}
        $master function load $code
        $master fcall writefunc 1 testkey testvalue
        wait_for_ofs_sync $master $replica
        
        # Verify the write was replicated
        assert_equal [$replica get testkey] {testvalue}
    }

    # Test FUNCTION DELETE replication
    test {FUNCTION DELETE replicates correctly via RREPLAY} {
        set code {#!lua name=deletelib
redis.register_function('delfunc', function(keys, args) return 1 end)}
        $master function load $code
        wait_for_ofs_sync $master $replica
        
        $master function delete deletelib
        wait_for_ofs_sync $master $replica
        
        # Verify deletion was replicated
        set libs [$replica function list]
        assert_no_match "*deletelib*" $libs
    }

    # Test HPERSIST replication
    test {HPERSIST replicates correctly via RREPLAY} {
        $master del myhash
        $master hset myhash field1 value1
        $master hexpire myhash 100 FIELDS 1 field1
        wait_for_ofs_sync $master $replica
        
        $master hpersist myhash FIELDS 1 field1
        wait_for_ofs_sync $master $replica
        
        # Verify persist was replicated
        set ttl [$replica httl myhash FIELDS 1 field1]
        assert_equal {-1} $ttl
    }

    # Test that read-only commands don't replicate
    test {EVAL_RO does not trigger replication} {
        $master set rokey "readonly"
        wait_for_ofs_sync $master $replica
        
        set offset_before [$replica info replication]
        $master eval_ro {return redis.call('GET', KEYS[1])} 1 rokey
        after 100
        set offset_after [$replica info replication]
        
        # Offsets should be the same (no replication)
        assert_match "*master_repl_offset:*" $offset_before
        assert_match "*master_repl_offset:*" $offset_after
    }

    # Test blocking commands replication
    test {BLMPOP replicates when unblocked} {
        $master del blocklist
        
        # Start blocking operation in background
        set rd [redis_deferring_client]
        $rd blmpop 5 1 blocklist LEFT COUNT 1
        
        # Push data to unblock
        after 100
        $master rpush blocklist x
        
        # Wait for result
        assert_equal [$rd read] {blocklist x}
        $rd close
        
        # Verify replication
        wait_for_ofs_sync $master $replica
        assert_equal [$replica llen blocklist] 0
    }

    # Test SINTERCARD doesn't replicate (read-only)
    test {SINTERCARD does not trigger replication} {
        $master del set1 set2
        $master sadd set1 a b c
        $master sadd set2 b c d
        wait_for_ofs_sync $master $replica
        
        set offset_before [$replica info replication]
        set card [$master sintercard 2 set1 set2]
        assert_equal $card 2
        after 100
        set offset_after [$replica info replication]
        
        # Offsets should be the same
        assert_match "*master_repl_offset:*" $offset_before
    }

    # Cleanup
    test {Cleanup replication setup} {
        $replica replicaof no one
    }
}}

# Test multi-master active-active scenario
start_server {tags {"replication multimaster"}} {
start_server {} {
    set master1 [srv -1 client]
    set master1_host [srv -1 host]
    set master1_port [srv -1 port]
    set master2 [srv 0 client]
    set master2_host [srv 0 host]
    set master2_port [srv 0 port]

    # Setup bidirectional active-active replication
    test {Setup multi-master replication} {
        $master1 config set active-replica yes
        $master2 config set active-replica yes
        $master1 replicaof $master2_host $master2_port
        $master2 replicaof $master1_host $master1_port
        wait_for_sync $master1
        wait_for_sync $master2
    }

    # Test Redis 8 commands in multi-master setup
    test {LMPOP works correctly in multi-master} {
        $master1 del mmlist
        $master1 rpush mmlist 1 2 3 4 5
        wait_for_ofs_sync $master1 $master2
        
        # Pop from master1
        $master1 lmpop 1 mmlist LEFT COUNT 2
        wait_for_ofs_sync $master1 $master2
        
        # Pop from master2
        $master2 lmpop 1 mmlist RIGHT COUNT 1
        wait_for_ofs_sync $master2 $master1
        
        # Both should be synchronized
        set len1 [$master1 llen mmlist]
        set len2 [$master2 llen mmlist]
        assert_equal $len1 $len2
        assert_equal $len1 2
    }

    # Test function libraries in multi-master
    test {Functions synchronize across multi-master} {
        $master1 function flush
        set code {#!lua name=mmlib
redis.register_function('mmfunc', function(keys, args)
    return 'multimaster'
end)}
        $master1 function load $code
        wait_for_ofs_sync $master1 $master2
        
        # Both masters should have the function
        assert_match "*mmlib*" [$master1 function list]
        assert_match "*mmlib*" [$master2 function list]
        
        # Both should be able to execute
        assert_equal [$master1 fcall mmfunc 0] {multimaster}
        assert_equal [$master2 fcall mmfunc 0] {multimaster}
    }

    # Test hash field expiry in multi-master
    test {Hash field expiry synchronizes across multi-master} {
        $master1 del mmhash
        $master1 hset mmhash f1 v1 f2 v2
        $master1 hexpire mmhash 100 FIELDS 2 f1 f2
        wait_for_ofs_sync $master1 $master2
        
        # Check expiry on both masters
        set ttl1 [$master1 httl mmhash FIELDS 1 f1]
        set ttl2 [$master2 httl mmhash FIELDS 1 f1]
        
        # Both should have TTL set
        assert {[lindex $ttl1 0] > 0 && [lindex $ttl1 0] <= 100}
        assert {[lindex $ttl2 0] > 0 && [lindex $ttl2 0] <= 100}
    }

    # Cleanup
    test {Cleanup multi-master setup} {
        $master1 replicaof no one
        $master2 replicaof no one
    }
}}

