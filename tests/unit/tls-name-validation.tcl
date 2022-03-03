test {TLS: Able to connect with no whitelist} {
    start_server {tags {"tls"}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect with whitelist '*'} {
    start_server {tags {"tls"} overrides {tls-whitelist *}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect with matching CN} {
    start_server {tags {"tls"} overrides {tls-whitelist client.keydb.dev}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect with matching SAN} {
    start_server {tags {"tls"} overrides {tls-whitelist san1.keydb.dev}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect with matching CN with wildcard} {
    start_server {tags {"tls"} overrides {tls-whitelist client*.dev}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect with matching SAN with wildcard} {
    start_server {tags {"tls"} overrides {tls-whitelist san*.dev}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect while with CN having a comprehensive list} {
    start_server {tags {"tls"} overrides {tls-whitelist {dummy.keydb.dev client.keydb.dev other.keydb.dev}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect while with SAN having a comprehensive list} {
    start_server {tags {"tls"} overrides {tls-whitelist {dummy.keydb.dev san2.keydb.dev other.keydb.dev}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect while with CN having a comprehensive list with wildcards} {
    start_server {tags {"tls"} overrides {tls-whitelist {dummy.* client*.dev other.*}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to connect while with SAN having a comprehensive list with wildcards} {
    start_server {tags {"tls"} overrides {tls-whitelist {dummy.* san*.dev other.*}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Not matching CN or SAN rejected} {
    start_server {tags {"tls"} overrides {tls-whitelist {client.keydb.dev}}} {
        catch {set r2 [redis_client_tls -keyfile "$::tlsdir/client2.key" -certfile "$::tlsdir/client2.crt" -require 1 -cafile "$::tlsdir/ca.crt"]} e
        assert_match {*I/O error reading reply*} $e
    }
}

test {TLS: Able to match against DNS SAN} {
    start_server {tags {"tls"} overrides {tls-whitelist {san1.keydb.dev}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to match against email SAN} {
    start_server {tags {"tls"} overrides {tls-whitelist {someone@keydb.dev}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to match against IPv4 SAN} {
    start_server {tags {"tls"} overrides {tls-whitelist {192.168.0.1}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to match against IPv4 with a wildcard} {
    start_server {tags {"tls"} overrides {tls-whitelist {192.*}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}

test {TLS: Able to match against URI SAN} {
    start_server {tags {"tls"} overrides {tls-whitelist {https://keydb.dev}}} {
        catch {r PING} e
        assert_match {PONG} $e
    }
}
