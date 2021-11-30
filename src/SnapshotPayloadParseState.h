#pragma once
#include <stack>
#include <vector>

class SlabAllocator;

class SnapshotPayloadParseState {
    enum class ParseStageName {
        None,
        Global,
        MetaData,
        Databases,
        Dataset,
        KeyValuePair,
    };
    struct ParseStage {
        ParseStageName name;
        long long arraylen = 0;
        long long arraycur = 0;
        std::array<std::pair<char*, size_t>, 2> arrvalues;

        ParseStage() {
            for (auto &pair : arrvalues) {
                pair.first = nullptr;
                pair.second = 0;
            }
        }
    };

    std::vector<ParseStage> stackParse;
    
    
    std::vector<char*> vecqueuedKeys;
    std::vector<size_t> vecqueuedKeysCb;
    std::vector<char*> vecqueuedVals;
    std::vector<size_t> vecqueuedValsCb;
    
    
    std::atomic<int> insertsInFlight;
    std::unique_ptr<SlabAllocator> m_spallocator;
    dict *dictLongLongMetaData = nullptr;
    dict *dictMetaData = nullptr;
    size_t cbQueued = 0;
    static const size_t queuedBatchLimit = 64*1024*1024;    // 64 MB
    int current_database = -1;

    ParseStageName getNextStage();
    void flushQueuedKeys();

public:
    SnapshotPayloadParseState();
    ~SnapshotPayloadParseState();

    long long getMetaDataLongLong(const char *field) const;
    sds getMetaDataStr(const char *szField) const;

    static const char *getStateDebugName(ParseStage stage);

    size_t depth() const;

    void trimState();
    void pushArray(long long size);
    void pushValue(const char *rgch, long long cch);
    void pushValue(long long value);
    bool shouldThrottle() const { return insertsInFlight > (cserver.cthreads*4); }
};