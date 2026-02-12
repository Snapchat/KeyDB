start_server {tags {"functions redis8"}} {
    test {FUNCTION STATS returns engine information} {
        set result [r FUNCTION STATS]
        assert_match "*engines*" $result
    }

    test {FUNCTION LIST on empty server} {
        r FUNCTION FLUSH
        set result [r FUNCTION LIST]
        assert_equal {} $result
    }

    test {FUNCTION FLUSH works} {
        r FUNCTION FLUSH
        set result [r FUNCTION LIST]
        assert_equal {} $result
    }

    test {FUNCTION KILL returns expected error when no script running} {
        catch {r FUNCTION KILL} err
        assert_match "*No scripts in execution*" $err
    }
}
