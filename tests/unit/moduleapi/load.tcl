set testmodule [file normalize tests/modules/load.so]

if {$::flash_enabled} {
    start_server [list tags [list "modules"] overrides [list storage-provider {flash ./rocks.db.master.load.test} databases 256 loadmodule $testmodule]] {
        test "Module is notified of keys loaded from flash" {
            r flushall
            r set foo bar
            r set bar foo
            r set foobar barfoo
            assert_equal [r load.count] 0
            r debug reload
            assert_equal [r load.count] 3
        }
    }
}