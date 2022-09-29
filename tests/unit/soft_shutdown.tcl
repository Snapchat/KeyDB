start_server {tags {"soft_shutdown"} } {
    test {soft shutdown command replies} {
        assert_equal [r shutdown soft] "OK"
    }

    test {soft shutdown errors on ping} {
        catch {[r ping]} e
        assert_match {SHUTDOWN PENDING} $e
    }
}

start_server {tags {"soft_shutdown"} } {
    test {soft shutdown prevents new connections} {
        assert_equal [r shutdown soft] "OK"
        # reconnect
        set catch_res [catch {set rd [redis_deferring_client]} e]
        if {$::tls} {
            assert_equal $catch_res 1
        } else {
            assert_match {*SHUTDOWN*} $e
        }
    }
}

start_server {tags {"soft_shutdown"} } {
    test {soft shutdown allows commands to execute while waiting} {
        assert_equal [r shutdown soft] "OK"
        r set test val
        assert_equal [r get test] {val}
    }
}

start_server {tags {"soft_shutdown"} } {
    test {soft shutdown shuts down after all clients exit} {
        assert_equal [r shutdown soft] "OK"
        r close
        after 500
        catch {set rd [redis_deferring_client]} e
        assert_match {*refused*} $e
    }
}

start_server {tags {"soft_shutdown"} overrides {soft-shutdown yes} } {
    test {soft shutdown triggered by SIGINT} {
        exec kill -SIGINT [s process_id]
        after 100
        catch {[r ping]} e
        assert_match {SHUTDOWN PENDING} $e
    }

    test {second SIGINT forces a shutdown during a soft shutdown} {
        exec kill -SIGINT [s process_id]
        after 100
        catch {[r ping]} e
        assert_match {*I/O*} $e
    }
}

start_server {tags {"soft_shutdown"} } {
    test {monitor does not prevent soft shutdown} {
        set monitor [redis_deferring_client]
        $monitor monitor
        assert_equal [r shutdown soft] "OK"
        r close
        after 500
        catch {set rd [redis_deferring_client]} e
        assert_match {*refused*} $e
    }
}

start_server {tags {"soft_shutdown"} } {
    start_server {} {
        set node_0 [srv 0 client]
        set node_0_host [srv 0 host]
        set node_0_port [srv 0 port]
        set node_0_pid [srv 0 pid]

        set node_1 [srv -1 client]
        set node_1_host [srv -1 host]
        set node_1_port [srv -1 port]
        set node_1_pid [srv -1 pid]

        $node_0 replicaof $node_1_host $node_1_port
        wait_for_sync $node_0

        test {soft shutdown works for with master} {
            $node_1 shutdown soft
        } {OK}

        test {soft shutdown on master doesn't affect replica} {
            assert_equal [$node_0 ping] {PONG}
        }

        test {soft shutdown on master updates ping response} {
            catch {$node_1 ping} e
            assert_equal $e {SHUTDOWN PENDING}
        }

        test {master prevents new connections with soft shutdown} {
            set c1 [redis $node_1_host $node_1_port 1 $::tls]
            set catch_res [catch {$c1 read} e]
            if {$::tls} {
                assert_equal $catch_res 1
            } else {
                assert_match {*SHUTDOWN*} $e
            }
        }

        test {master soft shutdown works after all clients disconnect} {
            $node_1 close
            after 500
            catch {set c1 [redis $node_1_host $node_1_port 1 $::tls]} e
            assert_match {*refused*} $e
        }
    }
}

start_server {tags {"soft_shutdown"} } {
    start_server {} {
        set node_0 [srv 0 client]
        set node_0_host [srv 0 host]
        set node_0_port [srv 0 port]
        set node_0_pid [srv 0 pid]

        set node_1 [srv -1 client]
        set node_1_host [srv -1 host]
        set node_1_port [srv -1 port]
        set node_1_pid [srv -1 pid]

        $node_0 replicaof $node_1_host $node_1_port
        wait_for_sync $node_0

        test {soft shutdown on replica is not blocked by master} {
            assert_equal [$node_0 shutdown soft] {OK}
            $node_0 close
            after 500
            catch {set c0 [redis $node_0_host $node_0_port 1 $::tls]} e
            assert_match {*refused*} $e
        }
    }
}