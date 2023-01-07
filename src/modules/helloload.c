/* Server hooks API example
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2019, Salvatore Sanfilippo <antirez at gmail dot com>
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

#define REDISMODULE_EXPERIMENTAL_API
#include "../redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

size_t count, finalCount;

/* Client state change callback. */
void loadCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(e);
    REDISMODULE_NOT_USED(data);

    if (sub == REDISMODULE_SUBEVENT_LOADING_FLASH_START || sub == REDISMODULE_SUBEVENT_LOADING_RDB_START || sub == REDISMODULE_SUBEVENT_LOADING_AOF_START || sub == REDISMODULE_SUBEVENT_LOADING_REPL_START) {
        count = 0;
        finalCount = 0;
    } else if (sub == REDISMODULE_SUBEVENT_LOADING_ENDED) {
        finalCount = count;
    }
}

int loadKeyCallback(RedisModuleCtx *ctx, int type,  const char *event, RedisModuleString *key) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(type);
    REDISMODULE_NOT_USED(event);

    const char *keyname = RedisModule_StringPtrLen(key, NULL);

    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_NOTICE, "Loaded key: %s", keyname);

    count++;
    return 0;
}

int HelloLoadCheck_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 1) return RedisModule_WrongArity(ctx);

    RedisModule_ReplyWithLongLong(ctx, RedisModule_DbSize(ctx) == finalCount);

    return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"hellohook",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModule_SubscribeToServerEvent(ctx,
        RedisModuleEvent_Loading, loadCallback);
    RedisModule_SubscribeToKeyspaceEvents(ctx,
        REDISMODULE_NOTIFY_LOADED, loadKeyCallback);

    if (RedisModule_CreateCommand(ctx,"helloload.check",
        HelloLoadCheck_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
