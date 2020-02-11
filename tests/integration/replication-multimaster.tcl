foreach topology {mesh ring} {
start_server {tags {"multi-master"} overrides {hz 500 active-replica yes multi-master yes}} {
start_server {overrides {hz 500 active-replica yes multi-master yes}} {
start_server {overrides {hz 500 active-replica yes multi-master yes}} {
start_server {overrides {hz 500 active-replica yes multi-master yes}} {

    for {set j 0} {$j < 4} {incr j} {
        set R($j) [srv [expr 0-$j] client]
        set R_host($j) [srv [expr 0-$j] host]
        set R_port($j) [srv [expr 0-$j] port]
    }

    # Initialize as mesh
    if [string equal $topology "mesh"] {
    for {set j 0} {$j < 4} {incr j} {
        for {set k 0} {$k < 4} {incr k} {
            if $j!=$k {
                $R($j) replicaof $R_host($k) $R_port($k)
                after 100
            }
        }
    }}
    #Else Ring
    if [string equal $topology "ring"] {
        $R(0) replicaof $R_host(3) $R_port(3)
        after 100
        $R(1) replicaof $R_host(0) $R_port(0)
        after 100
        $R(2) replicaof $R_host(1) $R_port(1)
        after 100
        $R(3) replicaof $R_host(2) $R_port(2)
    }

    after 2000

    test "$topology replicates to all nodes" {
        $R(0) set testkey foo
        after 500
        assert_equal foo [$R(1) get testkey] "replicates to 1"
        assert_equal foo [$R(2) get testkey] "replicates to 2"
    }

    test "$topology replicates only once" {
        $R(0) set testkey 1
        after 500
        $R(1) incr testkey
        after 500
        $R(2) incr testkey
        after 500
        assert_equal 3 [$R(0) get testkey]
        assert_equal 3 [$R(1) get testkey]
        assert_equal 3 [$R(2) get testkey]
        assert_equal 3 [$R(3) get testkey]
    }

    test "$topology transaction replicates only once" {
        for {set j 0} {$j < 1000} {incr j} {
            $R(0) set testkey 1
            $R(0) multi
            $R(0) incr testkey
            $R(0) incr testkey
            $R(0) exec
            after 1
            assert_equal 3 [$R(0) get testkey] "node 0"
            assert_equal 3 [$R(1) get testkey] "node 1"
            assert_equal 3 [$R(2) get testkey] "node 2"
            assert_equal 3 [$R(3) get testkey] "node 3"
        }
    }
}
}
}
}
}
