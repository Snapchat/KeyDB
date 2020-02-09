
start_server {tags {"repl"}} {
        test "incr of expired key on replica doesn't cause a crash" {
            r debug force-master yes
            r set testkey 1
            r pexpire testkey 1
            after 500
            r incr testkey
            r incr testkey
            r debug force-master no
        }
}
