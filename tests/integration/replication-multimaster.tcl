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

    test "$topology all nodes up" {
        for {set j 0} {$j < 4} {incr j} {
            wait_for_condition 50 100 {
                [string match {*all_master_link_status:up*} [$R($j) info replication]]
            } else {
                fail "Multimaster group didn't connect up in a reasonable period of time"
            }
        }
    }

    test "$topology replicates to all nodes" {
        $R(0) set testkey foo
        after 500
        for {set n 0} {$n < 4} {incr n} {
            wait_for_condition 50 100 {
                [$R($n) get testkey] == "foo"
            } else {
                fail "Failed to replicate to $n"
            }
        }
    }

    test "$topology replicates only once" {
        $R(0) set testkey 1
        after 500
        #wait_for_condition 50 100 {
        #    [$R(1) get testkey] == 1 && [$R(2) get testkey] == 1
        #} else {
        #    fail "Set failed to replicate"
        #}
        $R(1) incr testkey
        after 500
        $R(2) incr testkey
        after 500
        for {set n 0} {$n < 4} {incr n} {
            wait_for_condition 100 100 {
                [$R($n) get testkey] == 3
            } else {
                fail "node $n did not replicate"
            }
        }
    }

    test "$topology transaction replicates only once" {
        for {set j 0} {$j < 1000} {incr j} {
            $R(0) set testkey 1
            $R(0) multi
            $R(0) incr testkey
            $R(0) incr testkey
            $R(0) exec
	    for {set n 0} {$n < 4} {incr n} {
	        wait_for_condition 50 100 {
                    [$R($n) get testkey] == 3
                } else {
                    fail "node $n failed to replicate"
                }
            }
        }
    }
}
}
}
}
}
