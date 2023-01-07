set testmodule [file normalize tests/modules/load.so]

if {$::flash_enabled} {
    start_server {tags {"modules"} overrides {storage-provider {flash ./rocks.db.master} databases 256}} {
        r flushall
        r set foo bar
        r set bar foo
        r set foobar barfoo
    }
    start_server {tags {"modules"} overrides {storage-provider {flash ./rocks.db.master} databases 256}} {
        test "Module is notified of keys loaded from flash" {
            r load.check
        } {1}
    }
}