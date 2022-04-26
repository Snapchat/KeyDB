start_server {tags {"repl"} overrides {active-replica {yes} multi-master {yes}}} {
    start_server {overrides {active-replica {yes} multi-master {yes}}} {
        test {2 node cluster heals after multimaster psync} {
            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set replica [srv 0 client]
            set replica_host [srv 0 host]
            set replica_port [srv 0 port]

            # connect two nodes in active-active
            $replica replicaof $master_host $master_port
            $master replicaof $replica_host $replica_port
            after 1000

            # write to db7 in the master
            $master select 7
            $master set x 1

            # restart the replica to break the connection and force a psync
            restart_server 0 true false
            set replica [srv 0 client]

            # write again to db7
            $master set y 2

            # uncommenting the following delay causes test to pass
            # after 1000

            # reconnect the replica to the master
            $replica replicaof $master_host $master_port

            # verify results
            after 1000
            for {set j 0} {$j < 16} {incr j} {
                $master select $j
                $replica select $j
                assert_equal [$replica get x] [$master get x] 
                assert_equal [$replica get y] [$master get y]
            }
        }
    }
}