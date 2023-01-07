if {$::flash_enabled} {
    start_server {tags {"modules"} overrides {storage-provider {flash ./rocks.db.master} databases 256}} {
        r flushall
        r set foo bar
        r set bar foo
        r set foobar barfoo
    }
    start_server {tags {"modules"} overrides {storage-provider {flash ./rocks.db.master} databases 256 loadmodule tests/modules/load.so module-notify-flash-load yes}} {
        test "Module is notified of keys loaded from flash" {
            assert_equal [r load.count] [r dbsize]
        }
    }
}