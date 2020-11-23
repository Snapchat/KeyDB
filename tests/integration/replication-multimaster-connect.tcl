start_server {tags {"multi-master"} overrides {active-replica yes multi-master yes}} {
start_server {overrides {active-replica yes multi-master yes}} {
start_server {overrides {active-replica yes multi-master yes}} {
start_server {overrides {active-replica yes multi-master yes}} {
    for {set j 0} {$j < 4} {incr j} {
        set R($j) [srv [expr 0-$j] client]
        set R_host($j) [srv [expr 0-$j] host]
        set R_port($j) [srv [expr 0-$j] port]
    }

    set keysPerServer 100

    # Initialize Dataset
    for {set j 1} {$j < 4} {incr j} {
        for {set key 0} { $key < $keysPerServer} { incr key } {
            $R($j) set "key_$j\_$key" asdjaoijioasdjiod ex 100000
        }
        set hash($j) [$R($j) debug digest]
    }

    $R(1) replicaof $R_host(0) $R_port(0)
    $R(2) replicaof $R_host(0) $R_port(0)
    $R(3) replicaof $R_host(0) $R_port(0)

    test "all nodes up" {
        for {set j 1} {$j < 4} {incr j} {
            wait_for_condition 50 100 {
                [string match {*master_global_link_status:up*} [$R($j) info replication]]
            } else {
                fail "Multimaster group didn't connect up in a reasonable period of time"
            }
        }
    }

    test "nodes retain their data" {
        for {set j 1} { $j < 4 } { incr j } {
            assert_equal [$R($j) debug digest] $hash($j) $j
        }
    }

    # Set all servers with an overlapping key - the last one should win
    $R(0) set isvalid no
    $R(1) set isvalid no
    $R(2) set isvalid no
    # Note: Sleep is due to mvcc slip
    after 2
    $R(3) set isvalid yes

    for {set n 1} {$n < 4} {incr n} {
        test "Node $n reciprocal rep works" {
            $R(0) replicaof $R_host($n) $R_port($n)
            after 2000
            for {set key 0} { $key < $keysPerServer } { incr key } {
                assert_equal [$R(0) get "key_$n\_$key"] asdjaoijioasdjiod $key
            }
        }
    }

    test "All data transferred between nodes" {
        for {set server 0} {$server < 4} {incr server} {
            set hash($j) [$R($server) debug digest]
            for {set n 1} {$n < 4} {incr n} {
                for {set key 0} {$key < $keysPerServer} {incr key} {
                    assert_equal [$R($server) get "key_$n\_$key"] asdjaoijioasdjiod "server: $n key: $key"
                }
            }
        }
    }

    test "MVCC Updates Correctly" {
        assert_equal [$R(0) get isvalid] yes
        assert_equal [$R(1) get isvalid] yes
        assert_equal [$R(2) get isvalid] yes
        assert_equal [$R(3) get isvalid] yes
    }

    unset hash
    test "All servers same debug digest" {
        set hash [$R(0) debug digest]
        for {set j 1} {$j < 4} {incr j} {
            assert_equal $hash [$R($j) debug digest] $j
        }
    }
}}}}

# The tests below validate features replicated via RDB
start_server {tags {"multi-master"} overrides {active-replica yes multi-master yes}} {
start_server {overrides {active-replica yes multi-master yes}} {
start_server {overrides {active-replica yes multi-master yes}} {
    for {set j 0} {$j < 3} {incr j} {
        set R($j) [srv [expr 0-$j] client]
        set R_host($j) [srv [expr 0-$j] host]
        set R_port($j) [srv [expr 0-$j] port]
    }

    # Set replicated features here
    $R(0) sadd testhash subkey
    $R(0) expiremember testhash subkey 10000

    
    test "node 2 up" {
        $R(2) replicaof $R_host(1) $R_port(1)
        wait_for_condition 50 100 {
            [string match {*master_global_link_status:up*} [$R(2) info replication]]
        } else {
            fail "didn't connect up in a reasonable period of time"
        }
     }

    # While node 1 loads from 0, it will relay to 2
    test "node 1 up" {
        $R(1) replicaof $R_host(0) $R_port(0)
        wait_for_condition 50 100 {
            [string match {*master_global_link_status:up*} [$R(1) info replication]]
        } else {
            fail "didn't connect up in a reasonable period of time"
        }
     }

     #Tests that validate replication made it to node 2
     test "subkey expire replicates via RDB" {
         assert [expr [$R(2) ttl testhash subkey] > 0]
     }
}}}
