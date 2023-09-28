#include <string>
#include <fstream>
#include <limits>

static size_t getMemKey(std::string key) {
# ifdef __linux__
    std::string token;
    std::ifstream f("/proc/meminfo");
    while (f >> token) {
        if (token == key) {
            size_t mem_val;
            if (f >> mem_val) {
                return mem_val * 1024; // values are in kB
            } else {
                return 0;
            }
            f.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }
    return 0;
# else
    (void)key;
    return 0;
# endif
}

size_t getMemAvailable() {
    return getMemKey("MemAvailable:");
}

size_t getMemTotal() {
    return getMemKey("MemTotal:");
}