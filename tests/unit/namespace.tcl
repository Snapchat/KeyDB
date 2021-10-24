set server_path [tmpdir "server.namepsace"]
exec cp -f tests/assets/user-namespaces.acl $server_path
start_server [list overrides [list "dir" $server_path "aclfile" "user-namespaces.acl"]] {
    # user alice on ~* +@all -@dangerous -@admin >alice ::ns1
    # user bob on ~* +@all -@dangerous -@admin >bob ::ns2
    # user b2 on ~* +@all -@dangerous -@admin >b2 ::ns2
    # user default on nopass ~* +@all ::

    test {alice can set keys in multiple databases} {
        r AUTH alice alice
        r SELECT 0
        r SET name alice0
        r SELECT 1
        r SET name alice1
        r SELECT 2
        r SET name alice2
    }

    test {bob does not have acces to the latest db of alice} {
        r AUTH bob bob
        assert_equal "" [r GET name]
    }

    test {bob can set keys in multiple databases} {
        r AUTH bob bob
        r SELECT 0
        r SET name bob0
        r SELECT 1
        r SET name bob1
        r SELECT 2
        r SET name bob2
    }

    test {alice's keys are not overwritten by bob} {
        r AUTH alice alice
        r SELECT 0
        assert_equal "alice0" [r GET name]
        r SELECT 1
        assert_equal "alice1" [r GET name]
        r SELECT 2
        assert_equal "alice2" [r GET name]
    }

   test {bob's keys are not overwritten by alice} {
        r AUTH bob bob
        r SELECT 0
        assert_equal "bob0" [r GET name]
        r SELECT 1
        assert_equal "bob1" [r GET name]
        r SELECT 2
        assert_equal "bob2" [r GET name]
    }

    test {b2 can see bob's keys are not overwritten by alice} {
        r AUTH b2 b2
        r SELECT 0
        assert_equal "bob0" [r GET name]
        r SELECT 1
        assert_equal "bob1" [r GET name]
        r SELECT 2
        assert_equal "bob2" [r GET name]
    }
}

