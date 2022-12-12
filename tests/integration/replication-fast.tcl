proc prepare_value {size} {
    set _v "c"
    for {set i 1} {$i < $size} {incr i} {
        append _v 0
    }
    return $_v
}

if {$::flash_enabled} {
    start_server {tags {"replication-fast"} overrides {storage-provider {flash ./rocks.db.master} databases 256}} {
        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]
        start_server {tags {} overrides {storage-provider {flash ./rocks.db.replica} databases 256}} {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            test "fast replication with large value" {
                set _v [prepare_value [expr 64*1024*1024]]
                # $master set key $_v 

                $slave replicaof $master_host $master_port
                wait_for_condition 50 300 {
                    [lindex [$slave role] 0] eq {slave} &&
                    [string match {*master_link_status:up*} [$slave info replication]]
                } else {
                    fail "Can't turn the instance into a replica"
                }

                assert_equal [$slave debug digest] [$master debug digest]
                $slave replicaof no one
            }
        }
    }
}
