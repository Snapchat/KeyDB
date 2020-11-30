set server_path [tmpdir "server.rdb-encoding-test"]

# Store a bunch of datatypes to the database,
# compute the hash of the database,
# and save the data to a file
start_server  [list tags [list "loadsave"] overrides [list "dir" $server_path] keep_persistence true] {

    test "Save various data types to .rdb file" {
        r set "int" [expr {int(rand()*10000)}]
        r set "string" [string repeat A [expr {int(rand()*10000)}]]
        r hset "hash" [string repeat A [expr {int(rand()*1000)}]]  0[string repeat A [expr {int(rand()*1000)}]]
        r sadd "set" [string repeat A [expr {int(rand()*1000)}]]
        r zadd "zset" [expr {rand()}] [string repeat A [expr {int(rand()*1000)}]]
        r lpush "list" [string repeat A [expr {int(rand()*1000)}]]
        r keydb.cron "cron" single [expr {10000 + int(rand()*1000)}] "return 0" 0;# set delay long enough so it doesn't contend with saving
        set saved_digest [r debug digest];# debug digest computes the hash
        r save
    } {OK};
}

# Load that data back from the file,
# and compare its hash to the previously computed hash
start_server [list tags [list "loadsave"] overrides [list "dir" $server_path] keep_persistence true] {
    test "Load various data types from .rdb file" {
        set loaded_digest [r debug digest]
        if {![string match $saved_digest $loaded_digest]} {
            fail "Loaded data did not match saved data"
        }
    }
}

# Load in data from a redis instance
# The hash should match what we get in redis
set saved_digest 0cff3e9c86eb26ef3b5c0e6bac8315829ad6adf4
exec cp -f tests/assets/redis-save.rdb $server_path/dump.rdb 

start_server [list tags [list "loadsave"] overrides [list "dir" $server_path] keep_persistence true] {
    test "Load various data types from Redis generated .rdb file" {
        set loaded_digest [r debug digest]
        puts loaded_digest
        if {![string match $saved_digest $loaded_digest]} {
            fail "Loaded data did not match saved data"
        }
    }
}

