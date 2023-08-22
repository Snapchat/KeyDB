/*
 * Copyright (c) 2020, EQ Alpha Technology Ltd. <john at eqalpha dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include <math.h>

void dictObjectDestructor(void *privdata, void *val);
dictType nestedHashDictType {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictObjectDestructor,      /* val destructor */
};

robj *createNestHashBucket() {
    dict *d = dictCreate(&nestedHashDictType, nullptr);
    return createObject(OBJ_NESTEDHASH, d);
}

void freeNestedHashObject(robj_roptr o) {
    dictRelease((dict*)ptrFromObj(o));
}

class DbDictWrapper {
public:
    DbDictWrapper() = default;

    DbDictWrapper(dict *d)
        : m_dict(d)
    {}

    DbDictWrapper(redisDb *db)
        : m_db(db)
    {}

    dict_iter find(const char *key) {
        if (m_db != nullptr) {
            return m_db->find(key);
        } else if (m_dict != nullptr) {
            dictEntry *de = dictFind(m_dict, key);
            return dict_iter(m_dict, de);
        }
        return dict_iter(nullptr);
    }

    bool add(sds key, robj *val) {
        bool result = false;
        if (m_db != nullptr) {
            result = m_db->insert(key, val, true);
        } else if (m_dict != nullptr) {
            result = dictAdd(m_dict, key, val) == DICT_OK;
        }
        return result;
    }

private:
    redisDb *m_db = nullptr;
    dict *m_dict = nullptr;
};

robj *fetchFromKey(redisDb *db, robj_roptr key) {
    const char *pchCur = szFromObj(key);
    const char *pchStart = pchCur;
    const char *pchMax = pchCur + sdslen(pchCur);
    robj *o = nullptr;

    while (pchCur <= pchMax) {
        if (pchCur == pchMax || *pchCur == '.') {
            // WARNING: Don't deref pchCur as it may be pchMax

            // New word
            if ((pchCur - pchStart) < 1) {
                throw shared.syntaxerr; // malformed
            }

            DbDictWrapper srcDb;
            if (o == nullptr)
                srcDb = db;
            else
                srcDb = (dict*)ptrFromObj(o);
            
            sdsstring str(pchStart, pchCur - pchStart);
            o = srcDb.find(str.get()).val();

            if (o == nullptr) throw shared.nokeyerr;   // Not Found
            serverAssert(o->type == OBJ_NESTEDHASH || o->type == OBJ_STRING || o->type == OBJ_LIST);
            if (o->type == OBJ_STRING && pchCur != pchMax)
                throw shared.nokeyerr; // Past the end

            pchStart = pchCur + 1;
        }
        ++pchCur;
    }

    return o;
}

// Returns one if we overwrote a value
bool setWithKey(redisDb *db, robj_roptr key, robj *val, bool fCreateBuckets) {
    const char *pchCur = szFromObj(key);
    const char *pchStart = pchCur;
    const char *pchMax = pchCur + sdslen(pchCur);
    robj *o = nullptr;

    while (pchCur <= pchMax) {
        if (pchCur == pchMax || *pchCur == '.') {
            // WARNING: Don't deref pchCur as it may be pchMax

            // New word
            if ((pchCur - pchStart) < 1) {
                throw shared.syntaxerr; // malformed
            }

            DbDictWrapper src;
            if (o == nullptr)
                src = db;
            else
                src = (dict*)ptrFromObj(o);
            
            sdsstring str(pchStart, pchCur - pchStart);
            dict_iter di = src.find(str.get());

            if (pchCur == pchMax) {
                val->addref();
                if (di.val() != nullptr) {
                    decrRefCount(di.val());
                    di.setval(val);
                    return true;
                } else {
                    src.add(str.release(), val);
                    return false;
                }
            } else {
                o = di.val();

                if (o == nullptr) {
                    if (!fCreateBuckets)
                        throw shared.nokeyerr;   // Not Found
                    o = createNestHashBucket();
                    serverAssert(src.add(str.release(), o));
                } else if (o->type != OBJ_NESTEDHASH) {
                    decrRefCount(o);
                    o = createNestHashBucket();
                    di.setval(o);
                }
            }

            pchStart = pchCur + 1;
        }
        ++pchCur;
    }
    throw "Internal Error";
}

void writeNestedHashToClient(client *c, robj_roptr o) {
    if (o == nullptr) {
        addReply(c, shared.null[c->resp]);
    } else if (o->type == OBJ_STRING) {
        addReplyBulk(c, o);
    } else if (o->type == OBJ_LIST) {
        unsigned char *zl = (unsigned char*)ptrFromObj(o);
        addReplyArrayLen(c, ziplistLen(zl));
        unsigned char *p = ziplistIndex(zl, ZIPLIST_HEAD);
        while (p != nullptr) {
            unsigned char *str;
            unsigned int len;
            long long lval;
            if (ziplistGet(p, &str, &len, &lval)) {
                char rgT[128];
                if (str == nullptr) {
                    len = ll2string(rgT, 128, lval);
                    str = (unsigned char*)rgT;
                }
                addReplyBulkCBuffer(c, (const char*)str, len);
            }
            p = ziplistNext(zl, p);
        }
    } else {
        serverAssert(o->type == OBJ_NESTEDHASH );
        dict *d = (dict*)ptrFromObj(o);

        if (dictSize(d) > 1)
            addReplyArrayLen(c, dictSize(d));
        
        dictIterator *di = dictGetIterator(d);
        dictEntry *de;
        while ((de = dictNext(di))) {
            robj_roptr oT = (robj*)dictGetVal(de);
            addReplyArrayLen(c, 2);
            addReplyBulkCBuffer(c, (sds)dictGetKey(de), sdslen((sds)dictGetKey(de)));
            if (oT->type == OBJ_STRING) {
                addReplyBulk(c, oT);
            } else {
                writeNestedHashToClient(c, oT);
            }
        }
        dictReleaseIterator(di);
    }
}

inline bool FSimpleJsonEscapeCh(char ch) {
    return (ch == '"' || ch == '\\');
}
inline bool FExtendedJsonEscapeCh(char ch) {
    return ch <= 0x1F;
}

sds writeJsonValue(sds output, const char *valIn, size_t cchIn) {
    const char *val = valIn;
    size_t cch = cchIn;
    int cchEscapeExtra = 0;

    // First scan for escaped chars
    for (size_t ich = 0; ich < cchIn; ++ich) {
        if (FSimpleJsonEscapeCh(valIn[ich])) {
            ++cchEscapeExtra;
        } else if (FExtendedJsonEscapeCh(valIn[ich])) {
            cchEscapeExtra += 5;
        }
    }

    if (cchEscapeExtra > 0) {
        size_t ichDst = 0;
        sds dst = sdsnewlen(SDS_NOINIT, cchIn+cchEscapeExtra);
        for (size_t ich = 0; ich < cchIn; ++ich) {
            switch (valIn[ich]) {
                case '"':
                    dst[ichDst++] = '\\'; dst[ichDst++] = '"';
                    break;
                case '\\':
                    dst[ichDst++] = '\\'; dst[ichDst++] = '\\';
                    break;
                
                default:
                    serverAssert(!FSimpleJsonEscapeCh(valIn[ich]));
                    if (FExtendedJsonEscapeCh(valIn[ich])) {
                        dst[ichDst++] = '\\'; dst[ichDst++] = 'u';
                        snprintf(dst + ichDst, cchIn+cchEscapeExtra-ichDst, "%4x", valIn[ich]);
                        ichDst += 4;
                    } else {
                        dst[ichDst++] = valIn[ich];
                    }
                    break;
            }
        }
        val = (const char*)dst;
        serverAssert(ichDst == (cchIn+cchEscapeExtra));
        cch = ichDst;
    }

    output = sdscat(output, "\"");
    output = sdscatlen(output, val, cch);
    output = sdscat(output, "\"");

    if (val != valIn)
        sdsfree(val);

    return output;
}
sds writeJsonValue(sds output, sds val) {
    return writeJsonValue(output, (const char*)val, sdslen(val));
}

sds writeNestedHashAsJson(sds output, robj_roptr o) {
    if (o->type == OBJ_STRING) {
        output = writeJsonValue(output, (sds)szFromObj(o));
    } else if (o->type == OBJ_LIST) {
        unsigned char *zl = (unsigned char*)ptrFromObj(o);
        output = sdscat(output, "[");
        unsigned char *p = ziplistIndex(zl, ZIPLIST_HEAD);
        bool fFirst = true;
        while (p != nullptr) {
            unsigned char *str;
            unsigned int len;
            long long lval;
            if (ziplistGet(p, &str, &len, &lval)) {
                char rgT[128];
                if (str == nullptr) {
                    len = ll2string(rgT, 128, lval);
                    str = (unsigned char*)rgT;
                }
                if (!fFirst)
                    output = sdscat(output, ",");
                fFirst = false;
                output = writeJsonValue(output, (const char*)str, len);
            }
            p = ziplistNext(zl, p);
        }
        output = sdscat(output, "]");
    } else {
        output = sdscat(output, "{");
        dictIterator *di = dictGetIterator((dict*)ptrFromObj(o));
        dictEntry *de;
        bool fFirst = true;
        while ((de = dictNext(di))) {
            robj_roptr oT = (robj*)dictGetVal(de);
            if (!fFirst)
                output = sdscat(output, ",");
            fFirst = false;
            output = writeJsonValue(output, (sds)dictGetKey(de));
            output = sdscat(output, " : ");
            output = writeNestedHashAsJson(output, oT);
        }
        dictReleaseIterator(di);
        output = sdscat(output, "}");
    }
    return output;
}

void nhsetCommand(client *c) {
    if (c->argc < 3)
        throw shared.syntaxerr;
    
    robj *val = c->argv[2];
    if (c->argc > 3) {
        // Its a list, we'll store as a ziplist
        val = createZiplistObject();
        for (int iarg = 2; iarg < c->argc; ++iarg) {
            sds arg = (sds)szFromObj(c->argv[iarg]);
            val->m_ptr = ziplistPush((unsigned char*)ptrFromObj(val), (unsigned char*)arg, sdslen(arg), ZIPLIST_TAIL);
        }
    }

    try {
        if (setWithKey(c->db, c->argv[1], val, true)) {
            addReplyLongLong(c, 1); // we replaced a value
        } else {
            addReplyLongLong(c, 0); // we added a new value
        }
    } catch (...) {
        if (val != c->argv[2])
            decrRefCount(val);
        throw;
    }
    if (val != c->argv[2])
        decrRefCount(val);
}

void nhgetCommand(client *c) {
    if (c->argc != 2 && c->argc != 3)
        throw shared.syntaxerr;

    bool fJson = false;
    int argOffset = 0;
    if (c->argc == 3) {
        argOffset++;
        if (strcasecmp(szFromObj(c->argv[1]), "json") == 0) {
            fJson = true;
        } else if (strcasecmp(szFromObj(c->argv[1]), "resp") != 0)  {
            throw shared.syntaxerr;
        }
    }

    robj *o = fetchFromKey(c->db, c->argv[argOffset + 1]);
    if (fJson) {
        sds val = writeNestedHashAsJson(sdsnew(nullptr), o);
        addReplyBulkSds(c, val);
    } else { 
        writeNestedHashToClient(c, o);
    }
}