start_server {tags {"nested_hash"}} {
    test {Simple set/get} {
        assert_equal [r keydb.nhset foo bar] 0
        assert_equal bar [r keydb.nhget foo]
    }

    test {overwrite} {
        r flushall
        assert_equal [r keydb.nhset foo bar] 0
        assert_equal [r keydb.nhset foo baz] 1
        assert_equal [r keydb.nhget foo] baz
    }

    test {get simple string} {
        r keydb.nhset foo bar
        assert_equal [r keydb.nhget json foo] {"bar"}
        assert_equal [r keydb.nhget foo] "bar"
    }

    test {get array} {
        r keydb.nhset foo a b c d
        assert_equal [r keydb.nhget json foo] {["a","b","c","d"]}
        assert_equal [r keydb.nhget foo] {a b c d}
    }

    test {overwrite string with object} {
        r keydb.nhset a.b.c val1
        r keydb.nhset a.b val2
        r keydb.nhset a.b.c.d val3
        assert_equal [r keydb.nhget json a] {{"b" : {"c" : {"d" : "val3"}}}}
    }

    test {malformed nested key} {
        assert_error *syntax* {r keydb.nhset a..b val1}
    }

    test {missing final selector key} {
        assert_error *syntax* {r keydb.nhset a.b. val1}
    }

    test {expire nested hash} {
        r keydb.nhset a.b.c val1
        assert_equal [r expire a 100] 1
        assert [expr [r ttl a] > 0]
    }

    test {expire subhash} {
        r keydb.nhset a.b.c val1
        assert_equal [r expire a.b 100] 0
    }

    # Save not implemented so ensure we don't crash saving the RDB on shutdown
    r flushall
}
