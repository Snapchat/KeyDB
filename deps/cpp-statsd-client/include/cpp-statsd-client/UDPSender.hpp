#ifndef UDP_SENDER_HPP
#define UDP_SENDER_HPP

#ifdef _WIN32
#define NOMINMAX
#include <io.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

namespace Statsd {

#ifdef _WIN32
using SOCKET_TYPE = SOCKET;
constexpr SOCKET_TYPE k_invalidSocket{INVALID_SOCKET};
#define SOCKET_ERRNO WSAGetLastError()
#define SOCKET_CLOSE closesocket
#else
using SOCKET_TYPE = int;
constexpr SOCKET_TYPE k_invalidSocket{-1};
#define SOCKET_ERRNO errno
#define SOCKET_CLOSE close
#endif

/*!
 *
 * UDP sender
 *
 * A simple UDP sender handling batching.
 *
 */
class UDPSender final {
public:
    //!@name Constructor and destructor, non-copyable
    //!@{

    //! Constructor
    UDPSender(const std::string& host,
              const uint16_t port,
              const uint64_t batchsize,
              const uint64_t sendInterval) noexcept;

    //! Destructor
    ~UDPSender();

    UDPSender(const UDPSender&) = delete;
    UDPSender& operator=(const UDPSender&) = delete;
    UDPSender(UDPSender&&) = delete;

    //!@}

    //!@name Methods
    //!@{

    //! Send or enqueue a message
    void send(const std::string& message) noexcept;

    //! Returns the error message as a string
    const std::string& errorMessage() const noexcept;

    //! Returns true if the sender is initialized
    bool initialized() const noexcept;

    //! Flushes any queued messages
    void flush() noexcept;

    //!@}

private:
    // @name Private methods
    // @{

    //! Initialize the sender and returns true when it is initialized
    bool initialize() noexcept;

    //! Queue a message to be sent to the daemon later
    inline void queueMessage(const std::string& message) noexcept;

    //! Send a message to the daemon
    void sendToDaemon(const std::string& message) noexcept;

    //!@}

private:
    // @name State variables
    // @{

    //! Shall we exit?
    std::atomic<bool> m_mustExit{false};

    //!@}

    // @name Network info
    // @{

    //! The hostname
    std::string m_host;

    //! The port
    uint16_t m_port;

    //! The structure holding the server
    struct sockaddr_in m_server;

    //! The socket to be used
    SOCKET_TYPE m_socket = k_invalidSocket;

    //!@}

    // @name Batching info
    // @{

    //! The batching size
    uint64_t m_batchsize;

    //! The sending frequency in milliseconds
    uint64_t m_sendInterval;

    //! The queue batching the messages
    std::deque<std::string> m_batchingMessageQueue;

    //! The mutex used for batching
    std::mutex m_batchingMutex;

    //! The thread dedicated to the batching
    std::thread m_batchingThread;

    //!@}

    //! Error message (optional string)
    std::string m_errorMessage;
};

namespace detail {

inline bool isValidSocket(const SOCKET_TYPE socket) {
    return socket != k_invalidSocket;
}

#ifdef _WIN32
struct WinSockSingleton {
    inline static const WinSockSingleton& getInstance() {
        static const WinSockSingleton instance;
        return instance;
    }
    inline bool ok() const {
        return m_ok;
    }
    ~WinSockSingleton() {
        WSACleanup();
    }

private:
    WinSockSingleton() {
        WSADATA wsa;
        m_ok = WSAStartup(MAKEWORD(2, 2), &wsa) == 0;
    }
    bool m_ok;
};
#endif

}  // namespace detail

inline UDPSender::UDPSender(const std::string& host,
                            const uint16_t port,
                            const uint64_t batchsize,
                            const uint64_t sendInterval) noexcept
    : m_host(host), m_port(port), m_batchsize(batchsize), m_sendInterval(sendInterval) {
    // Initialize the socket
    if (!initialize()) {
        return;
    }

    // If batching is on, use a dedicated thread to send after the wait time is reached
    if (m_batchsize != 0 && m_sendInterval > 0) {
        // Define the batching thread
        m_batchingThread = std::thread([this] {
            // TODO: this will drop unsent stats, should we send all the unsent stats before we exit?
            while (!m_mustExit.load(std::memory_order_acquire)) {
                std::deque<std::string> stagedMessageQueue;

                std::unique_lock<std::mutex> batchingLock(m_batchingMutex);
                m_batchingMessageQueue.swap(stagedMessageQueue);
                batchingLock.unlock();

                // Flush the queue
                while (!stagedMessageQueue.empty()) {
                    sendToDaemon(stagedMessageQueue.front());
                    stagedMessageQueue.pop_front();
                }

                // Wait before sending the next batch
                std::this_thread::sleep_for(std::chrono::milliseconds(m_sendInterval));
            }
        });
    }
}

inline UDPSender::~UDPSender() {
    if (!initialized()) {
        return;
    }

    // If we're running a background thread tell it to stop
    if (m_batchingThread.joinable()) {
        m_mustExit.store(true, std::memory_order_release);
        m_batchingThread.join();
    }

    // Cleanup the socket
    SOCKET_CLOSE(m_socket);
}

inline void UDPSender::send(const std::string& message) noexcept {
    m_errorMessage.clear();

    // If batching is on, accumulate messages in the queue
    if (m_batchsize > 0) {
        queueMessage(message);
        return;
    }

    // Or send it right now
    sendToDaemon(message);
}

inline void UDPSender::queueMessage(const std::string& message) noexcept {
    // We aquire a lock but only if we actually need to (i.e. there is a thread also accessing the queue)
    auto batchingLock =
        m_batchingThread.joinable() ? std::unique_lock<std::mutex>(m_batchingMutex) : std::unique_lock<std::mutex>();
    // Either we don't have a place to batch our message or we exceeded the batch size, so make a new batch
    if (m_batchingMessageQueue.empty() || m_batchingMessageQueue.back().length() > m_batchsize) {
        m_batchingMessageQueue.emplace_back();
        m_batchingMessageQueue.back().reserve(m_batchsize + 256);
    }  // When there is already a batch open we need a separator when its not empty
    else if (!m_batchingMessageQueue.back().empty()) {
        m_batchingMessageQueue.back().push_back('\n');
    }
    // Add the new message to the batch
    m_batchingMessageQueue.back().append(message);
}

inline const std::string& UDPSender::errorMessage() const noexcept {
    return m_errorMessage;
}

inline bool UDPSender::initialize() noexcept {
#ifdef _WIN32
    if (!detail::WinSockSingleton::getInstance().ok()) {
        m_errorMessage = "WSAStartup failed: errno=" + std::to_string(SOCKET_ERRNO);
    }
#endif

    // Connect the socket
    m_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (!detail::isValidSocket(m_socket)) {
        m_errorMessage = "socket creation failed: errno=" + std::to_string(SOCKET_ERRNO);
        return false;
    }

    std::memset(&m_server, 0, sizeof(m_server));
    m_server.sin_family = AF_INET;
    m_server.sin_port = htons(m_port);

    if (inet_pton(AF_INET, m_host.c_str(), &m_server.sin_addr) == 0) {
        // An error code has been returned by inet_aton

        // Specify the criteria for selecting the socket address structure
        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_DGRAM;

        // Get the address info using the hints
        struct addrinfo* results = nullptr;
        const int ret{getaddrinfo(m_host.c_str(), nullptr, &hints, &results)};
        if (ret != 0) {
            // An error code has been returned by getaddrinfo
            SOCKET_CLOSE(m_socket);
            m_socket = k_invalidSocket;
            m_errorMessage = "getaddrinfo failed: err=" + std::to_string(ret) + ", msg=" + gai_strerror(ret);
            return false;
        }

        // Copy the results in m_server
        struct sockaddr_in* host_addr = (struct sockaddr_in*)results->ai_addr;
        std::memcpy(&m_server.sin_addr, &host_addr->sin_addr, sizeof(struct in_addr));

        // Free the memory allocated
        freeaddrinfo(results);
    }

    return true;
}

inline void UDPSender::sendToDaemon(const std::string& message) noexcept {
    // Try sending the message
    const auto ret = sendto(m_socket,
                            message.data(),
#ifdef _WIN32
                            static_cast<int>(message.size()),
#else
                            message.size(),
#endif
                            0,
                            (struct sockaddr*)&m_server,
                            sizeof(m_server));
    if (ret == -1) {
        m_errorMessage = "sendto server failed: host=" + m_host + ":" + std::to_string(m_port) +
                         ", err=" + std::to_string(SOCKET_ERRNO);
    }
}

inline bool UDPSender::initialized() const noexcept {
    return m_socket != k_invalidSocket;
}

inline void UDPSender::flush() noexcept {
    // We aquire a lock but only if we actually need to (ie there is a thread also accessing the queue)
    auto batchingLock =
        m_batchingThread.joinable() ? std::unique_lock<std::mutex>(m_batchingMutex) : std::unique_lock<std::mutex>();
    // Flush the queue
    while (!m_batchingMessageQueue.empty()) {
        sendToDaemon(m_batchingMessageQueue.front());
        m_batchingMessageQueue.pop_front();
    }
}

}  // namespace Statsd

#endif
