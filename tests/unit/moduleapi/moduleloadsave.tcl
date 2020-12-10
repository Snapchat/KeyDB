set server_path [tmpdir "server.rdb-encoding-test"]
set testmodule [file normalize tests/modules/datatype.so]

# Store module data typed data to the database,
# compute the hash of the database,
# and save the data to a file
start_server  [list tags [list "loadsave"] overrides [list "dir" $server_path "loadmodule" $testmodule] keep_persistence true] {

    test "Save module data type to .rdb file" {
        r datatype.set key1 100 stringval
        r datatype.set key2 200 stringval
        r datatype.set key3 300 stringval
        r datatype.set key4 400 stringval
        r datatype.set key5 500 stringval
        set saved_digest [r debug digest];
        r save
    } {OK};
}

# Load that data back from the file,
# and compare its hash to the previously computed hash
start_server [list tags [list "loadsave"] overrides [list "dir" $server_path "loadmodule" $testmodule] keep_persistence true] {
    test "Load module data type from .rdb file" {
        set loaded_digest [r debug digest]
        if {![string match $saved_digest $loaded_digest]} {
            fail "Loaded data did not match saved data"
        }
    }
}

# Load in data from a redis instance
# The hash should match what we get in redis
set saved_digest acffad6b89e21339dc5c80f53f6c6fa15977a848
exec cp -f tests/assets/redis-module-save.rdb $server_path/dump.rdb 

start_server [list tags [list "loadsave"] overrides [list "dir" $server_path "loadmodule" $testmodule] keep_persistence true] {
    test "Load module data type from Redis generated .rdb file" {
        set loaded_digest [r debug digest]
        if {![string match $saved_digest $loaded_digest]} {
            fail "Loaded data did not match saved data"
        }
    }
}
