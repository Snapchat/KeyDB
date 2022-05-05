# Creates a master-replica pair and breaks the link continuously to force
# partial resyncs attempts, all this while flooding the master with
# write queries.
#
# You can specify backlog size, ttl, delay before reconnection, test duration
# in seconds, and an additional condition to verify at the end.
#
# If reconnect is > 0, the test actually try to break the connection and
# reconnect with the master, otherwise just the initial synchronization is
# checked for consistency.
proc test_psync {descr duration backlog_size backlog_ttl delay cond mdl sdl reconnect} {
    start_server [list tags [list "repl"] overrides [list active-replica yes client-output-buffer-limit [list replica $backlog_size $backlog_size 9999999] ] ] {
        start_server [list overrides [list client-output-buffer-limit [list replica $backlog_size $backlog_size 9999999] active-replica yes ] ] {

            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set replica [srv 0 client]
            set replica_host [srv 0 host]
            set replica_port [srv 0 port]

            $master config set repl-backlog-size $backlog_size
            $master config set repl-backlog-ttl $backlog_ttl
            $master config set repl-diskless-sync $mdl
            $master config set repl-diskless-sync-delay 1
            $replica config set repl-diskless-load $sdl
            
            $replica config set repl-backlog-size $backlog_size
            $replica config set repl-backlog-ttl $backlog_ttl
            $replica config set repl-diskless-sync $mdl
            $replica config set repl-diskless-sync-delay 1
            $master config set repl-diskless-load $sdl

            test {Replica should be able to synchronize with the master} {
                $replica replicaof $master_host $master_port
            }

            after 1000

            test {Master should be able to synchronize with the replica} {
                $master replicaof $replica_host $replica_port
            }

            set load_handle0 [start_climbing_load $master_host $master_port 9 100000]
            set load_handle1 [start_climbing_load $master_host $master_port 11 100000]
            set load_handle2 [start_climbing_load $master_host $master_port 12 100000]

            # Check that the background clients are actually writing.
            test {Detect write load to master} {
                wait_for_condition 50 1000 {
                    [$master dbsize] > 100
                } else {
                    fail "Can't detect write load from background clients."
                }
            }

            test "Test replication partial resync: $descr (diskless: $mdl, $sdl, reconnect: $reconnect)" {
                # Now while the clients are writing data, break the maste-replica
                # link multiple times.
                if ($reconnect) {
                    for {set j 0} {$j < $duration*10} {incr j} {
                        after 100
                        # catch {puts "MASTER [$master dbsize] keys, REPLICA [$replica dbsize] keys"}

                        if {($j % 20) == 0} {
                            catch {
                                if {$delay} {
                                    $replica multi
                                    $replica client kill $master_host:$master_port
                                    $replica debug sleep $delay
                                    $replica exec
                                } else {
                                    $replica client kill $master_host:$master_port
                                }
                            }
                        }
                    }
                }
                stop_climbing_load $load_handle0
                stop_climbing_load $load_handle1
                stop_climbing_load $load_handle2

                # Wait for the replica to reach the "online"
                # state from the POV of the master.
                set retry 5000
                while {$retry} {
                    set info [$master info]
                    if {[string match {*slave0:*state=online*} $info]} {
                        break
                    } else {
                        incr retry -1
                        after 100
                    }
                }
                if {$retry == 0} {
                    error "assertion:replica not correctly synchronized"
                }

                # Wait that replica acknowledge it is online so
                # we are sure that DBSIZE and DEBUG DIGEST will not
                # fail because of timing issues. (-LOADING error)
                wait_for_condition 5000 100 {
                    [lindex [$replica role] 3] eq {connected}
                } else {
                    fail "replica still not connected after some time"
                }

                wait_for_condition 100 100 {
                    [$master debug digest] == [$replica debug digest]
                } else {
                    set csv1 [csvdump r]
                    set csv2 [csvdump {r -1}]
                    set fd [open /tmp/repldump1.txt w]
                    puts -nonewline $fd $csv1
                    close $fd
                    set fd [open /tmp/repldump2.txt w]
                    puts -nonewline $fd $csv2
                    close $fd
                    fail "Master - Replica inconsistency, Run diff -u against /tmp/repldump*.txt for more info"
                }
                assert {[$master dbsize] > 0}
                eval $cond
            }
        }
    }
}


foreach mdl {no yes} {
    foreach sdl {disabled swapdb} {
        test_psync {no reconnection, just sync} 6 1000000 3600 0 {
        } $mdl $sdl 0

        test_psync {ok psync} 6 100000000 3600 0 {
        assert {[s -1 sync_partial_ok] > 0}
        } $mdl $sdl 1

        test_psync {no backlog} 6 100 3600 0.5 {
        assert {[s -1 sync_partial_err] > 0}
        } $mdl $sdl 1

        test_psync {ok after delay} 3 100000000 3600 3 {
        assert {[s -1 sync_partial_ok] > 0}
        } $mdl $sdl 1

        test_psync {backlog expired} 3 100000000 1 3 {
        assert {[s -1 sync_partial_err] > 0}
        } $mdl $sdl 1
    }
}
