start_server {tags {"rreplay"} overrides {active-replica yes}} {

    test {RREPLAY use current db} {
        r debug force-master yes
        r select 4
        r set dbnum invalid
        r rreplay "f4d5b2b5-4f07-4ee5-a4f2-5dc98507dfce" "*3\r\n\$3\r\nSET\r\n\$5\r\ndbnum\r\n\$4\r\nfour\r\n"
        r get dbnum
    } {four}
    reconnect

    test {RREPLAY db different} {
        r debug force-master yes
        r select 4
        r set testkey four
        r rreplay "f4d5b2b5-4f07-4ee5-a4f2-5dc98507dfce" "*3\r\n\$3\r\nSET\r\n\$7\r\ntestkey\r\n\$4\r\nbebe\r\n" 2
        r select 4
        assert { [r get testkey] == "four" }
        r select 2
        r get testkey
    } {bebe}

    reconnect

    test {RREPLAY not master} {
        assert_error "*master*" {r rreplay "f4d5b2b5-4f07-4ee5-a4f2-5dc98507dfce" "*3\r\n\$3\r\nSET\r\n\$7\r\ntestkey\r\n\$4\r\nbebe\r\n" 2}
    }

    r flushdb
}
