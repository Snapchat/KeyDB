# C++ StatsD Client

![logo](https://raw.githubusercontent.com/vthiery/cpp-statsd-client/master/images/logo.svg?sanitize=true)

[![Release](https://img.shields.io/github/release/vthiery/cpp-statsd-client.svg?style=for-the-badge)](https://github.com/vthiery/cpp-statsd-client/releases/latest)
![License](https://img.shields.io/github/license/vthiery/cpp-statsd-client?style=for-the-badge)
[![Linux status](https://img.shields.io/github/workflow/status/vthiery/cpp-statsd-client/Linux?label=Linux&style=for-the-badge)](https://github.com/vthiery/cpp-statsd-client/actions/workflows/linux.yml?query=branch%3Amaster++)
[![Windows status](https://img.shields.io/github/workflow/status/vthiery/cpp-statsd-client/Windows?label=Windows&style=for-the-badge)](https://github.com/vthiery/cpp-statsd-client/actions/workflows/windows.yml?query=branch%3Amaster++)

A header-only StatsD client implemented in C++.
The client allows:

- batching,
- change of configuration at runtime,
- user-defined frequency rate.

## Install and Test

### Makefile

In order to install the header files and/or run the tests, simply use the Makefile and execute

```sh
make install
```

and

```sh
make test
```

### Conan

If you are using [Conan](https://www.conan.io/) to manage your dependencies, merely add statsdclient/x.y.z@vthiery/stable to your conanfile.py's requires, where x.y.z is the release version you want to use. Please file issues here if you experience problems with the packages. You can also directly download the latest version [here](https://bintray.com/vthiery/conan-packages/statsdclient%3Avthiery/_latestVersion).

## Usage

### Example

A simple example of how to use the client:

```cpp
#include "StatsdClient.hpp"
using namespace Statsd;

int main() {
    // Define the client on localhost, with port 8080,
    // using a prefix,
    // a batching size of 20 bytes,
    // and three points of precision for floating point gauge values
    StatsdClient client{ "127.0.0.1", 8080, "myPrefix", 20, 3 };

    // Increment the metric "coco"
    client.increment("coco");

    // Decrement the metric "kiki"
    client.decrement("kiki");

    // Adjusts "toto" by +3
    client.count("toto", 2, 0.1f);

    // Record a gauge "titi" to 3
    client.gauge("titi", 3);

    // Record a timing of 2ms for "myTiming"
    client.timing("myTiming", 2, 0.1f);

    // Send a metric explicitly
    client.send("tutu", 4, "c", 2.0f);
    exit(0);
}
```

### Advanced Testing

A simple mock StatsD server can be found at `tests/StatsdServer.hpp`. This can be used to do simple validation of your application's metrics, typically in the form of unit tests. In fact this is the primary means by which this library is tested. The mock server itself is not distributed with the library so to use it you'd need to vendor this project into your project. Once you have though, you can test your application's use of the client like so:

```cpp
#include "StatsdClient.hpp"
#include "StatsdServer.hpp"

#include <cassert>

using namespace Statsd;

struct MyApp {
    void doWork() const {
        m_client.count("bar", 3);
    }
private:
    StatsdClient m_client{"localhost", 8125, "foo"};
};

int main() {
    StatsdServer mockServer;

    MyApp app;
    app.doWork();

    assert(mockServer.receive() == "foo.bar:3|c");
    exit(0);
}
```

### Configuration

The configuration of the client must be input when one instantiates it. Nevertheless, the API allows the configuration ot change afterwards. For example, one can do the following:

```cpp
#include "StatsdClient.hpp"
using namespace Statsd;

int main()
{
    // Define the client on localhost, with port 8080,
    // using a prefix,
    // a batching size of 20 bytes,
    // and three points of precision for floating point gauge values
    StatsdClient client{ "127.0.0.1", 8080, "myPrefix", 20, 3 };

    client.increment("coco");

    // Set a new configuration, using a different port, a different prefix, and more gauge precision
    client.setConfig("127.0.0.1", 8000, "anotherPrefix", 6);

    client.decrement("kiki");
}
```

The batchsize is the only parameter that cannot be changed for the time being.

### Batching

The client supports batching of the metrics. The batch size parameter is the number of bytes to allow in each batch (UDP datagram payload) to be sent to the statsd process. This number is not a hard limit. If appending the current stat to the current batch (separated by the `'\n'` character) pushes the current batch over the batch size, that batch is enqueued (not sent) and a new batch is started. If batch size is 0, the default, then each stat is sent individually to the statsd process and no batches are enqueued.

### Sending

As previously mentioned, if batching is disabled (by setting the batch size to 0) then every stat is sent immediately in a blocking fashion. If batching is enabled (ie non-zero) then you may also set the send interval. The send interval controls the time, in milliseconds, to wait before flushing/sending the queued stats batches to the statsd process. When the send interval is non-zero a background thread is spawned which will do the flushing/sending at the configured send interval, in other words asynchronously. The queuing mechanism in this case is *not* lock-free. If batching is enabled but the send interval is set to zero then the queued batchs of stats will not be sent automatically by a background thread but must be sent manually via the `flush` method. The `flush` method is a blocking call.


### Frequency rate

When sending a metric, a frequency rate can be set in order to limit the metrics' sampling. By default, the frequency rate is set to one and won't affect the sampling. If set to a value `epsilon` (0.0001 for the time being) close to one, the sampling is not affected either.

If the frequency rate is set and `epsilon` different from one, the sending will be rejected randomly (the higher the frequency rate, the lower the probability of rejection).

## License

This library is under MIT license.
