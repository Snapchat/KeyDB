#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <string.h>

static long current_timer_count = 0;
static long total_timer_count;

void timerHandler(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    current_timer_count++;
    RedisModule_Log(ctx, "notice", "Timer %ld went off", current_timer_count);
    if (current_timer_count == total_timer_count){
        RedisModule_Log(ctx, "notice", "All timers fired successfully");
    }
}

int elapsed(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    REDISMODULE_NOT_USED(argc);
    REDISMODULE_NOT_USED(argv);
    RedisModule_ReplyWithLongLong(ctx, (long long)current_timer_count);
    return REDISMODULE_OK;
} 

enum TimerOrder {
    TIMERS_ASCENDING,
    TIMERS_DESCENDING,
    TIMERS_SAME
};

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"timer-spawn",1,REDISMODULE_APIVER_1)
            == REDISMODULE_ERR) return REDISMODULE_ERR;

    enum TimerOrder mode;
    if (argc >= 1){
        const char * modeString = RedisModule_StringPtrLen(argv[0], NULL);
        if (strcmp(modeString, "ascending") == 0){
            mode = TIMERS_ASCENDING;
        } else if (strcmp(modeString, "descending") == 0){
            mode = TIMERS_DESCENDING;
        } else if (strcmp(modeString, "same") == 0){
            mode = TIMERS_SAME;
        } else {
            RedisModule_Log(ctx, "warning", "Invalid mode specified as first argument. Valid modes are: ascending, descending, and same");
            return REDISMODULE_ERR;
        }
    } else {
        mode = TIMERS_DESCENDING;
    }

    long long user_timer_count;
    total_timer_count = (argc >= 2 && RedisModule_StringToLongLong(argv[1], &user_timer_count) == REDISMODULE_OK) ? 
        (long)user_timer_count : 2500;

    if (RedisModule_CreateCommand(ctx,"timer.elapsed", elapsed,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    for (long i = 0; i < total_timer_count; i++){
        mstime_t period;
        if (mode == TIMERS_ASCENDING){
            period = i;
        } else if (mode == TIMERS_DESCENDING){
            period = total_timer_count - i;
        } else if (mode == TIMERS_SAME){
            period = total_timer_count;
        }
        RedisModule_CreateTimer(ctx, period, timerHandler, NULL);
    }

    return REDISMODULE_OK;
}
