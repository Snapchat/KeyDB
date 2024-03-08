# Cluster test suite. Copyright (C) 2014 Salvatore Sanfilippo antirez@gmail.com
# This software is released under the BSD License. See the COPYING file for
# more information.

cd tests/cluster
source cluster.tcl
source ../instances.tcl
source ../../support/cluster.tcl ; # Redis Cluster client.

set ::instances_count 20 ; # How many instances we use at max.
set ::tlsdir "../../tls"

# Check if we compiled with flash
set status [catch {exec ../../../src/keydb-server --is-flash-enabled}]
if {$status == 0} {
    puts "KeyDB was built with FLASH, including FLASH tests"
    set ::flash_enabled 1
} else {
    puts "KeyDB was not built with FLASH.  Excluding FLASH tests"
    set ::flash_enabled 0
}

proc main {} {
    parse_options
    spawn_instance redis $::redis_base_port $::instances_count {
	"bind 127.0.0.1"
	"cluster-enabled yes"
	"appendonly yes"
	"testmode yes"
	"server-threads 3"
    }
    run_tests
    cleanup
    end_tests
}

if {[catch main e]} {
    puts $::errorInfo
    if {$::pause_on_error} pause_on_error
    cleanup
    exit 1
}
