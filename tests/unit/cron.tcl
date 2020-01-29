start_server {tags {"CRON"} overrides {hz 100} } {
    test {cron singleshot past tense} {
        r flushall
        r cron testjob single 0 1 {redis.call("incr", "testkey")} 1 testkey
        after 300
        assert_equal 1 [r get testkey]
        assert_equal 0 [r exists testjob]
    }

    test {cron repeat past tense next exec is in the future} {
        r flushall
        r cron testjob repeat 0 1000000 {redis.call("incr", "testkey")} 1 testkey
        after 300
        assert_equal 1 [r get testkey]
        assert_equal 1 [r exists testjob]
        r del testjob
    }

    test {cron repeat works} {
        r flushall
        r cron testjob repeat 0 600 {redis.call("incr","testkey")}
        after 1000
        assert_equal 2 [r get testkey]
    }

    test {cron overwrite works} {
        r flushall
        r cron testjob single 500 {redis.call("set","testkey","a")} 1 testkey
        r cron testjob single 500 {redis.call("set","anotherkey","b")} 1 anotherkey
        after 1000
        assert_equal 0 [r exists testkey]
        assert_equal b [r get anotherkey]
    }

    test {cron delete key stops job} {
        r flushall
        r cron testjob single 500 {redis.call("set","testkey","a")}
        r del testjob
        after 1000
        assert_equal 0 [r exists testkey]
    }

    test {cron zero interval rejected} {
        catch {r cron testjob single 0 0 {redis.call("incr","testkey")} 1 testkey} e
        assert_match {ERR*} $e
    }
}
