#pragma once

struct cronjob
{
    sdsstring script;
    uint64_t interval;
    uint64_t startTime;
    std::vector<sdsstring> veckeys;
    std::vector<sdsstring> vecargs;
    int dbNum = 0;
    bool fSingleShot = false;
};

void freeCronObject(robj_roptr o);
void executeCronJobExpireHook(const char *key, robj *o);
void cronCommand(client *c);