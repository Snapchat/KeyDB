# only run this test if tls is enabled
if {$::tls} {
    package require tls

    test {TLS Audit Log: Able to connect with no exclustion list} {
            start_server {tags {"tls"}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with exclusion list '*'} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist *}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with matching CN} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist client.keydb.dev}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with matching SAN} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist san1.keydb.dev}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with matching CN with wildcard} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist client*.dev}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with matching SAN with wildcard} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist san*.dev}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect while with CN having a comprehensive list} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {dummy.keydb.dev client.keydb.dev other.keydb.dev}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect while with SAN having a comprehensive list} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {dummy.keydb.dev san2.keydb.dev other.keydb.dev}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect while with CN having a comprehensive list with wildcards} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {dummy.* client*.dev other.*}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect while with SAN having a comprehensive list with wildcards} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {dummy.* san*.dev other.*}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Not matching CN or SAN accepted} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {client.keydb.dev}}} {
	    catch {r PING}
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to match against DNS SAN} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {san1.keydb.dev}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to match against email SAN} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {someone@keydb.dev}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to match against IPv4 SAN} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {192.168.0.1}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to match against IPv4 with a wildcard} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist {192.*}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to match against URI SAN} {
        start_server {tags {"tls"} overrides {tls-allowlist {https://keydb.dev}}} {
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with matching CN} {
        start_server {tags {"tls"} overrides {tls-auditlog-blocklist test.dev}} {
            r set testkey foo
            wait_for_condition 50 1000 {
                [log_file_matches [srv 0 stdout] "*Audit Log: *, cmd set, keys: testkey*"]
            } else {
                fail "Missing expected Audit Log entry"
            }
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with matching TLS allowlist and Audit Log blocklist} {
        start_server {tags {"tls"} overrides {tls-allowlist client.keydb.dev tls-auditlog-blocklist client.keydb.dev}} {
            r set testkey foo
            if {[log_file_matches [srv 0 stdout] "*Audit Log: *, cmd set, keys: testkey*"]} {
                fail "Unexpected Audit Log entry"
            }
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

    test {TLS Audit Log: Able to connect with different TLS allowlist and Audit Log blocklist} {
        start_server {tags {"tls"} overrides {tls-allowlist client.keydb.dev tls-auditlog-blocklist test.dev}} {
            r set testkey foo
            wait_for_condition 50 1000 {
                [log_file_matches [srv 0 stdout] "*Audit Log: *, cmd set, keys: testkey*"]
            } else {
                fail "Missing expected Audit Log entry"
            }
            catch {r PING} e
            assert_match {PONG} $e
        }
    }

} else {
    start_server {} {
        # just a dummy server so that the test doesn't panic if tls is disabled
        # otherwise the test will try to bind to a server that just isn't there
    }
}
