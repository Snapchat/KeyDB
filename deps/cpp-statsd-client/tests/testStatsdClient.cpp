#include <iostream>

#include "StatsdServer.hpp"
#include "cpp-statsd-client/StatsdClient.hpp"

using namespace Statsd;

// Each test suite below spawns a thread to recv the client messages over UDP as if it were a real statsd server
// Note that we could just synchronously recv metrics and not use a thread but doing the test async has the
// advantage that we can test the threaded batching mode in a straightforward way. The server thread basically
// just keeps storing metrics in an vector until it hears a special one signaling the test is over and bails
void mock(StatsdServer& server, std::vector<std::string>& messages) {
    do {
        // Grab the messages that are waiting
        auto recvd = server.receive();

        // Split the messages on '\n'
        auto start = std::string::npos;
        do {
            // Keep this message
            auto end = recvd.find('\n', ++start);
            messages.emplace_back(recvd.substr(start, end));
            start = end;

            // Bail if we found the special quit message
            if (messages.back().find("DONE") != std::string::npos) {
                messages.pop_back();
                return;
            }
        } while (start != std::string::npos);
    } while (server.errorMessage().empty() && !messages.back().empty());
}

template <typename SocketWrapper>
void throwOnError(const SocketWrapper& wrapped, bool expectEmpty = true, const std::string& extraMessage = "") {
    if (wrapped.errorMessage().empty() != expectEmpty) {
        std::cerr << (expectEmpty ? wrapped.errorMessage() : extraMessage) << std::endl;
        throw std::runtime_error(expectEmpty ? wrapped.errorMessage() : extraMessage);
    }
}

void throwOnWrongMessage(StatsdServer& server, const std::string& expected) {
    auto actual = server.receive();
    if (actual != expected) {
        std::cerr << "Expected: " << expected << " but got: " << actual << std::endl;
        throw std::runtime_error("Incorrect stat received");
    }
}

void testErrorConditions() {
    // Resolve a rubbish ip and make sure initialization failed
    StatsdClient client{"256.256.256.256", 8125, "myPrefix", 20};
    throwOnError(client, false, "Should not be able to resolve a ridiculous ip");
}

void testReconfigure() {
    StatsdServer server;
    throwOnError(server);

    StatsdClient client("localhost", 8125, "first.");
    client.increment("foo");
    throwOnWrongMessage(server, "first.foo:1|c");

    client.setConfig("localhost", 8125, "second");
    client.increment("bar");
    throwOnWrongMessage(server, "second.bar:1|c");

    client.setConfig("localhost", 8125, "");
    client.increment("third.baz");
    throwOnWrongMessage(server, "third.baz:1|c");

    client.increment("");
    throwOnWrongMessage(server, ":1|c");

    // TODO: test what happens with the batching after resolving the question about incomplete
    //  batches being dropped vs sent on reconfiguring
}

void testSendRecv(uint64_t batchSize, uint64_t sendInterval) {
    StatsdServer mock_server;
    std::vector<std::string> messages, expected;
    std::thread server(mock, std::ref(mock_server), std::ref(messages));

    // Set a new config that has the client send messages to a proper address that can be resolved
    StatsdClient client("localhost", 8125, "sendRecv.", batchSize, sendInterval, 3);
    throwOnError(client);

    // TODO: I forget if we need to wait for the server to be ready here before sending the first stats
    //  is there a race condition where the client sending before the server binds would drop that clients message

    for (int i = 0; i < 3; ++i) {
        // Increment "coco"
        client.increment("coco");
        throwOnError(client);
        expected.emplace_back("sendRecv.coco:1|c");

        // Decrement "kiki"
        client.decrement("kiki");
        throwOnError(client);
        expected.emplace_back("sendRecv.kiki:-1|c");

        // Adjusts "toto" by +2
        client.seed(19);  // this seed gets a hit on the first call
        client.count("toto", 2, 0.1f);
        throwOnError(client);
        expected.emplace_back("sendRecv.toto:2|c|@0.10");

        // Gets "sampled out" by the random number generator
        client.count("popo", 9, 0.1f);
        throwOnError(client);

        // Record a gauge "titi" to 3
        client.gauge("titi", 3);
        throwOnError(client);
        expected.emplace_back("sendRecv.titi:3|g");

        // Record a gauge "titifloat" to -123.456789 with precision 3
        client.gauge("titifloat", -123.456789);
        throwOnError(client);
        expected.emplace_back("sendRecv.titifloat:-123.457|g");

        // Record a timing of 2ms for "myTiming"
        client.seed(19);
        client.timing("myTiming", 2, 0.1f);
        throwOnError(client);
        expected.emplace_back("sendRecv.myTiming:2|ms|@0.10");

        // Send a set with 1227 total uniques
        client.set("tutu", 1227, 2.0f);
        throwOnError(client);
        expected.emplace_back("sendRecv.tutu:1227|s");

        // Gauge but with tags
        client.gauge("dr.röstigrabe", 333, 1.f, {"liegt", "im", "weste"});
        throwOnError(client);
        expected.emplace_back("sendRecv.dr.röstigrabe:333|g|#liegt,im,weste");

        // All the things
        client.count("foo", -42, .9f, {"bar", "baz"});
        throwOnError(client);
        expected.emplace_back("sendRecv.foo:-42|c|@0.90|#bar,baz");
    }

    // Signal the mock server we are done
    client.timing("DONE", 0);

    // If manual flushing do it now
    if (sendInterval == 0) {
        client.flush();
    }

    // Wait for the server to stop
    server.join();

    // Make sure we get the exactly correct output
    if (messages != expected) {
        std::cerr << "Unexpected stats received by server, got:" << std::endl;
        for (const auto& message : messages) {
            std::cerr << message << std::endl;
        }
        std::cerr << std::endl << "But we expected:" << std::endl;
        for (const auto& message : expected) {
            std::cerr << message << std::endl;
        }
        throw std::runtime_error("Unexpected stats");
    }
}

int main() {
    // If any of these tests fail they throw an exception, not catching makes for a nonzero return code

    // general things that should be errors
    testErrorConditions();
    // reconfiguring how you are sending
    testReconfigure();
    // no batching
    testSendRecv(0, 0);
    // background batching
    testSendRecv(32, 1000);
    // manual flushing of batches
    testSendRecv(16, 0);

    return EXIT_SUCCESS;
}
