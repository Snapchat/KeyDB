#include "server.h"
#include "SnapshotPayloadParseState.h"
#include <sys/mman.h>

static const size_t SLAB_SIZE = 2*1024*1024;   // Note 64 MB because we try to give 64MB to a block
class SlabAllocator
{
    class Slab {
        char *m_pv = nullptr;
        size_t m_cb = 0;
        size_t m_cbAllocated = 0;

    public:
        Slab(size_t cbAllocate) {
            m_pv = (char*)zmalloc(cbAllocate);
            m_cb = cbAllocate;
        }

        ~Slab() {
            zfree(m_pv);
        }

        Slab(Slab &&other) {
            m_pv = other.m_pv;
            m_cb = other.m_cb;
            m_cbAllocated = other.m_cbAllocated;
            other.m_pv = nullptr;
            other.m_cb = 0;
            other.m_cbAllocated = 0;
        }

        bool canAllocate(size_t cb) const {
            return (m_cbAllocated + cb) <= m_cb;
        }

        void *allocate(size_t cb) {
            if ((m_cbAllocated + cb) > m_cb)
                return nullptr;
            void *pvret = m_pv + m_cbAllocated;
            m_cbAllocated += cb;
            return pvret;
        }
    };
    std::vector<Slab> m_vecslabs;
public:

    void *allocate(size_t cb) {
        if (m_vecslabs.empty() || !m_vecslabs.back().canAllocate(cb)) {
            m_vecslabs.emplace_back(std::max(cb, SLAB_SIZE));
        }
        return m_vecslabs.back().allocate(cb);
    }
};

static uint64_t dictCStringHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, strlen((char*)key));
}


static void dictKeyDestructor(void *privdata, void *key)
{
    DICT_NOTUSED(privdata);
    sdsfree((sds)key);
}

static int dictCStringCompare(void *, const void *key1, const void *key2)
{
    int l1,l2;

    l1 = strlen((sds)key1);
    l2 = strlen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

dictType metadataLongLongDictType = {
    dictCStringHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictCStringCompare,         /* key compare */
    dictKeyDestructor,          /* key destructor */
    nullptr,                    /* val destructor */
    nullptr,                    /* allow to expand */
    nullptr                     /* async free destructor */
};

dictType metadataDictType = {
    dictSdsHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictSdsDestructor,          /* val destructor */
    nullptr,                    /* allow to expand */
    nullptr                     /* async free destructor */
};


SnapshotPayloadParseState::ParseStageName SnapshotPayloadParseState::getNextStage() {
    if (stackParse.empty())
        return ParseStageName::Global;

    switch (stackParse.back().name)
    {
        case ParseStageName::None:
            return ParseStageName::Global;

        case ParseStageName::Global:
            if (stackParse.back().arraycur == 0)
                return ParseStageName::MetaData;
            else if (stackParse.back().arraycur == 1)
                return ParseStageName::Databases;
            break;

        case ParseStageName::MetaData:
            return ParseStageName::KeyValuePair;

        case ParseStageName::Databases:
            return ParseStageName::Dataset;

        case ParseStageName::Dataset:
            return ParseStageName::KeyValuePair;

        default:
            break;
    }
    throw "Bad protocol: corrupt state";
}

void SnapshotPayloadParseState::flushQueuedKeys() {
    if (vecqueuedKeys.empty())
        return;
    serverAssert(current_database >= 0);

    // TODO: We can't finish parse until all the work functions finish
    int idb = current_database;
    serverAssert(vecqueuedKeys.size() == vecqueuedVals.size());
    auto sizePrev = vecqueuedKeys.size();
    ++insertsInFlight;
    auto &insertsInFlightTmp = insertsInFlight; // C++ GRRRRRRRRRRRRRRRR, we don't want to capute "this" because that's dangerous
    if (current_database < cserver.dbnum) {
        g_pserver->asyncworkqueue->AddWorkFunction([idb, vecqueuedKeys = std::move(this->vecqueuedKeys), vecqueuedKeysCb = std::move(this->vecqueuedKeysCb), vecqueuedVals = std::move(this->vecqueuedVals), vecqueuedValsCb = std::move(this->vecqueuedValsCb), &insertsInFlightTmp, pallocator = m_spallocator.release()]() mutable {
            g_pserver->db[idb]->bulkStorageInsert(vecqueuedKeys.data(), vecqueuedKeysCb.data(), vecqueuedVals.data(), vecqueuedValsCb.data(), vecqueuedKeys.size());
            --insertsInFlightTmp;
            delete pallocator;
        });
    } else {
        // else drop the data
        vecqueuedKeys.clear();
        vecqueuedKeysCb.clear();
        vecqueuedVals.clear();
        vecqueuedValsCb.clear();
        // Note: m_spallocator will get free'd when overwritten below
    }
    m_spallocator = std::make_unique<SlabAllocator>();
    cbQueued = 0;
    vecqueuedKeys.reserve(sizePrev);
    vecqueuedKeysCb.reserve(sizePrev);
    vecqueuedVals.reserve(sizePrev);
    vecqueuedValsCb.reserve(sizePrev);
    serverAssert(vecqueuedKeys.empty());
    serverAssert(vecqueuedVals.empty());
}


SnapshotPayloadParseState::SnapshotPayloadParseState() {
    // This is to represent the first array the client is intended to send us
    ParseStage stage;
    stage.name = ParseStageName::None;
    stage.arraylen = 1;
    stackParse.push_back(stage);

    dictLongLongMetaData = dictCreate(&metadataLongLongDictType, nullptr);
    dictMetaData = dictCreate(&metadataDictType, nullptr);
    insertsInFlight = 0;
    m_spallocator = std::make_unique<SlabAllocator>();
}

SnapshotPayloadParseState::~SnapshotPayloadParseState() {
    dictRelease(dictLongLongMetaData);
    dictRelease(dictMetaData);
}

const char *SnapshotPayloadParseState::getStateDebugName(ParseStage stage) {
    switch (stage.name) {
        case ParseStageName::None:
            return "None";

        case ParseStageName::Global:
            return "Global";

        case ParseStageName::MetaData:
            return "MetaData";

        case ParseStageName::Databases:
            return "Databases";

        case ParseStageName::KeyValuePair:
            return "KeyValuePair";

        case ParseStageName::Dataset:
            return "Dataset";

        default:
            return "Unknown";
    }
}

size_t SnapshotPayloadParseState::depth() const { return stackParse.size(); }

void SnapshotPayloadParseState::trimState() {
    while (!stackParse.empty() && (stackParse.back().arraycur == stackParse.back().arraylen))
        stackParse.pop_back();
    
    if (stackParse.empty()) {
        flushQueuedKeys();
        while (insertsInFlight > 0) {
            // TODO: ProcessEventsWhileBlocked
            aeReleaseLock();
            aeAcquireLock();
        }
    }
}

void SnapshotPayloadParseState::pushArray(long long size) {
    if (stackParse.empty())
        throw "Bad Protocol: unexpected trailing data";

    if (size == 0) {
        stackParse.back().arraycur++;
        return;
    }
    
    if (stackParse.back().name == ParseStageName::Databases) {
        flushQueuedKeys();
        current_database = stackParse.back().arraycur;
    }

    ParseStage stage;
    stage.name = getNextStage();
    stage.arraylen = size;

    if (stage.name == ParseStageName::Dataset) {
        g_pserver->db[current_database]->expand(stage.arraylen);
    }

    // Note: This affects getNextStage so ensure its after
    stackParse.back().arraycur++;
    stackParse.push_back(stage);
}

void SnapshotPayloadParseState::pushValue(const char *rgch, long long cch) {
    if (stackParse.empty())
        throw "Bad Protocol: unexpected trailing bulk string";

    if (stackParse.back().arraycur >= static_cast<int>(stackParse.back().arrvalues.size()))
        throw "Bad protocol: Unexpected value";

    auto &stage = stackParse.back();
    stage.arrvalues[stackParse.back().arraycur].first = (char*)m_spallocator->allocate(cch);
    stage.arrvalues[stackParse.back().arraycur].second = cch;
    memcpy(stage.arrvalues[stackParse.back().arraycur].first, rgch, cch);
    stage.arraycur++;
    switch (stage.name) {
        case ParseStageName::KeyValuePair:
            if (stackParse.size() < 2)
                throw "Bad Protocol: unexpected bulk string";
            if (stackParse[stackParse.size()-2].name == ParseStageName::MetaData) {
                if (stage.arraycur == 2) {
                    // We loaded both pairs
                    if (stage.arrvalues[0].first == nullptr || stage.arrvalues[1].first == nullptr)
                        throw "Bad Protocol: Got array when expecing a string"; // A baddy could make us derefence the vector when its too small

                    if (!strcasecmp(stage.arrvalues[0].first, "lua")) {
                        /* Load the script back in memory. */
                        robj *auxval = createStringObject(stage.arrvalues[1].first, stage.arrvalues[1].second);
                        if (luaCreateFunction(NULL,g_pserver->lua,auxval) == NULL) {
                            throw "Can't load Lua script";
                        }
                    } else {
                        dictAdd(dictMetaData, sdsnewlen(stage.arrvalues[0].first, stage.arrvalues[0].second), sdsnewlen(stage.arrvalues[1].first, stage.arrvalues[1].second));
                    }
                }
            } else if (stackParse[stackParse.size()-2].name == ParseStageName::Dataset) {
                if (stage.arraycur == 2) {
                    // We loaded both pairs
                    if (stage.arrvalues[0].first == nullptr || stage.arrvalues[1].first == nullptr)
                        throw "Bad Protocol: Got array when expecing a string"; // A baddy could make us derefence the vector when its too small
                    vecqueuedKeys.push_back(stage.arrvalues[0].first);
                    vecqueuedKeysCb.push_back(stage.arrvalues[0].second);
                    vecqueuedVals.push_back(stage.arrvalues[1].first);
                    vecqueuedValsCb.push_back(stage.arrvalues[1].second);
                    stage.arrvalues[0].first = nullptr;
                    stage.arrvalues[1].first = nullptr;
                    cbQueued += vecqueuedKeysCb.back();
                    cbQueued += vecqueuedValsCb.back();
                    if (cbQueued >= queuedBatchLimit)
                        flushQueuedKeys();
                }
            } else {
                throw "Bad Protocol: unexpected bulk string";
            }
            break;

        default:
            throw "Bad Protocol: unexpected bulk string out of KV pair";
    }
}

void SnapshotPayloadParseState::pushValue(long long value) {
    if (stackParse.empty())
        throw "Bad Protocol: unexpected integer value";
    
    stackParse.back().arraycur++;

    if (stackParse.back().arraycur != 2 || stackParse.back().arrvalues[0].first == nullptr)
        throw "Bad Protocol: unexpected integer value";

    dictEntry *de = dictAddRaw(dictLongLongMetaData, sdsnewlen(stackParse.back().arrvalues[0].first, stackParse.back().arrvalues[0].second), nullptr);
    if (de == nullptr)
        throw "Bad Protocol: metadata field sent twice";
    de->v.s64 = value;
}

long long SnapshotPayloadParseState::getMetaDataLongLong(const char *szField) const {
    dictEntry *de = dictFind(dictLongLongMetaData, szField);

    if (de == nullptr) {
        serverLog(LL_WARNING, "Master did not send field: %s", szField);
        throw false;
    }
    return de->v.s64;
}

sds SnapshotPayloadParseState::getMetaDataStr(const char *szField) const {
    sdsstring str(szField, strlen(szField));

    dictEntry *de = dictFind(dictMetaData, str.get());
    if (de == nullptr) {
        serverLog(LL_WARNING, "Master did not send field: %s", szField);
        throw false;
    }
    return (sds)de->v.val;
}