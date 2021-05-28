set testmodule [file normalize tests/modules/timers.so]
set timercount 4000


tags "modules" {
    test {Ascending module timers can load in correctly} {
        start_server [list overrides [list loadmodule "$testmodule ascending $timercount"]] {
            wait_for_condition [expr round($timercount/10)] 20 {
                [r timer.elapsed] == $timercount
            } else {
                fail "Server failed to load in timers with ascending periods"
            }
        }
    }
    
    test {Descending module timers can load in correctly} {
        start_server [list overrides [list loadmodule "$testmodule descending $timercount"]] {
            wait_for_condition [expr round($timercount/10)] 20 {
                [r timer.elapsed] == $timercount
            } else {
                fail "Server failed to load in timers with descending periods"
            }
        }
    }

    test {Module timers with the same period can load in correctly} {
        start_server [list overrides [list loadmodule "$testmodule same $timercount"]] {
            wait_for_condition [expr round($timercount/10)] 20 {
                [r timer.elapsed] == $timercount
            } else {
                fail "Server failed to load in timers with the same period"
            }
        }
    }
}
