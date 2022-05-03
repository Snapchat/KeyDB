source tests/support/keydb.tcl


set ::tlsdir "tests/tls"

proc gen_climbing_load {host port db ops tls} {
    set start_time [clock seconds]
    set r [redis $host $port 1 $tls]
    $r client setname LOAD_HANDLER
    $r select $db
    set x 0
    while {$x < $ops} {
        incr x
        $r set [expr $x] [expr rand()]
    }
}

gen_climbing_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4]
