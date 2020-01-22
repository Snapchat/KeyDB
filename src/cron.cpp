#include "server.h"
#include "cron.h"

void freeCronObject(robj_roptr o)
{
    delete reinterpret_cast<const cronjob*>(ptrFromObj(o));
}

// CRON [name] [single shot] [optional: start] [delay] [script] [numkeys] [key N] [arg N]
void cronCommand(client *c)
{
    int arg_offset = 0;
    static const int ARG_NAME = 1;
    static const int ARG_SINGLESHOT = 2;
    static const int ARG_EXPIRE = 3;
    #define ARG_SCRIPT (4+arg_offset)
    #define ARG_NUMKEYS (5+arg_offset)
    #define ARG_KEYSTART (6+arg_offset)

    bool fSingleShot = false;
    if (strcasecmp("single", szFromObj(c->argv[ARG_SINGLESHOT])) == 0) {
        fSingleShot = true;
    } else {
        if (strcasecmp("repeat", szFromObj(c->argv[ARG_SINGLESHOT])) != 0) {
            addReply(c, shared.syntaxerr);
            return;
        }
    }

    long long interval;
    if (getLongLongFromObjectOrReply(c, c->argv[ARG_EXPIRE], &interval, "missing expire time") != C_OK)
        return;

    long long base = g_pserver->mstime;
    if (getLongLongFromObject(c->argv[ARG_EXPIRE+1], &base) == C_OK) {
        arg_offset++;
        std::swap(base, interval);
    }

    if (interval <= 0)
    {
        addReplyError(c, "interval must be positive");
        return;
    }

    long numkeys = 0;
    if (c->argc > ARG_NUMKEYS)
    {
        if (getLongFromObjectOrReply(c, c->argv[ARG_NUMKEYS], &numkeys, NULL) != C_OK)
            return;
        
        if (c->argc < (6 + numkeys)) {
            addReplyError(c, "Missing arguments or numkeys is too big");
        }
    }

    std::unique_ptr<cronjob> spjob = std::make_unique<cronjob>();
    spjob->script = sdsstring(sdsdup(szFromObj(c->argv[ARG_SCRIPT])));
    spjob->interval = (uint64_t)interval;
    spjob->startTime = (uint64_t)base;
    spjob->fSingleShot = fSingleShot;
    spjob->dbNum = c->db - g_pserver->db;
    for (long i = 0; i < numkeys; ++i)
        spjob->veckeys.emplace_back(sdsdup(szFromObj(c->argv[ARG_KEYSTART+i])));
    for (long i = ARG_KEYSTART + numkeys; i < c->argc; ++i)
        spjob->vecargs.emplace_back(sdsdup(szFromObj(c->argv[i])));

    robj *o = createObject(OBJ_CRON, spjob.release());
    setKey(c->db, c->argv[ARG_NAME], o);
    // use an expire to trigger execution.  Note: We use a subkey expire here so legacy clients don't delete it.
    setExpire(c, c->db, c->argv[ARG_NAME], c->argv[ARG_NAME], base + interval);
    addReply(c, shared.ok);
}

void executeCronJobExpireHook(const char *key, robj *o)
{
    serverAssert(o->type == OBJ_CRON);
    cronjob *job = (cronjob*)ptrFromObj(o);
    
    client *cFake = createClient(-1, IDX_EVENT_LOOP_MAIN);
    cFake->lock.lock();
    cFake->authenticated = 1;
    cFake->puser = nullptr;
    selectDb(cFake, job->dbNum);
    serverAssert(cFake->argc == 0);

    // Setup the args for the EVAL command
    cFake->argc = 3 + job->veckeys.size() + job->vecargs.size();
    cFake->argv = (robj**)zmalloc(sizeof(robj*) * cFake->argc, MALLOC_LOCAL);
    cFake->argv[0] = createStringObject("EVAL", 4);
    cFake->argv[1] = createStringObject(job->script.get(), job->script.size());
    cFake->argv[2] = createStringObjectFromLongLong(job->veckeys.size());
    for (size_t i = 0; i < job->veckeys.size(); ++i)
        cFake->argv[3+i] = createStringObject(job->veckeys[i].get(), job->veckeys[i].size());
    for (size_t i = 0; i < job->vecargs.size(); ++i)
        cFake->argv[3+job->veckeys.size()+i] = createStringObject(job->vecargs[i].get(), job->vecargs[i].size());

    evalCommand(cFake);
    resetClient(cFake);

    robj *keyobj = createStringObject(key,sdslen(key));
    int dbId = job->dbNum;
    if (job->fSingleShot)
    {
        dbSyncDelete(cFake->db, keyobj);
    }
    else
    {
        job->startTime += job->interval;
        if (job->startTime < (uint64_t)g_pserver->mstime)
        {
            // If we are more than one interval in the past then fast forward to
            //  the first interval still in the future
            auto delta = g_pserver->mstime - job->startTime;
            auto multiple = (delta / job->interval)+1;
            job->startTime += job->interval * multiple;
        }
        setExpire(cFake, cFake->db, keyobj, keyobj, job->startTime + job->interval);
    }

    notifyKeyspaceEvent(NOTIFY_KEYEVENT, "CRON Executed", keyobj, dbId);
    decrRefCount(keyobj);

    // o is invalid at this point
    freeClient(cFake);
}