/*
 * Functions API for KeyDB - Real Implementation  
 * Ported from Redis 8.2.3 functions.c
 * Adapted for KeyDB's C++ and multithreading architecture
 */

#include "server.h"
#include "sds.h"
#include "atomicvar.h"
#include <mutex>

/* Lua headers */
extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

#include "functions.h"  /* Include after other headers to ensure proper linkage */

/* Forward declaration from scripting.cpp */
void luaReplyToRedisReply(client *c, lua_State *lua);

#define LOAD_TIMEOUT_MS 500

/* Forward declarations */
static void engineFunctionDispose(void *privdata, void *obj);
static void engineStatsDispose(void *privdata, void *obj);
static void engineLibraryDispose(void *privdata, void *obj);
static void engineDispose(void *privdata, void *obj);
static int functionsVerifyName(sds name);

/* External dict helpers for case-insensitive engine name matching */
extern uint64_t dictSdsCaseHash(const void *key);
extern int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);

typedef struct functionsLibEngineStats {
    size_t n_lib;
    size_t n_functions;
} functionsLibEngineStats;

/* Global state - protected by mutex for thread-safety */
static dict *engines = NULL;
static functionsLibCtx *curr_functions_lib_ctx = NULL;
static std::mutex functions_mutex;  /* KeyDB: Thread safety */

/* Dictionary types - using case-insensitive hash/compare from dict.c */
dictType engineDictType = {
    dictSdsCaseHash,        /* hash function - case insensitive */
    NULL,                   /* key dup */
    NULL,                   /* val dup */
    dictSdsKeyCaseCompare,  /* key compare - case insensitive */
    dictSdsDestructor,      /* key destructor */
    engineDispose,          /* val destructor */
    NULL,                   /* allow to expand */
    NULL                    /* privdata */
};

dictType functionDictType = {
    dictSdsHash,            /* hash function */
    NULL,                   /* key dup */
    NULL,                   /* val dup */
    dictSdsKeyCompare,      /* key compare */
    dictSdsDestructor,      /* key destructor */
    NULL,                   /* val destructor */
    NULL,                   /* allow to expand */
    NULL                    /* privdata */
};

dictType engineStatsDictType = {
    dictSdsHash,            /* hash function */
    NULL,                   /* key dup */
    NULL,                   /* val dup */
    dictSdsKeyCompare,      /* key compare */
    dictSdsDestructor,      /* key destructor */
    engineStatsDispose,     /* val destructor */
    NULL,                   /* allow to expand */
    NULL                    /* privdata */
};

dictType libraryFunctionDictType = {
    dictSdsHash,            /* hash function */
    NULL,                   /* key dup */
    NULL,                   /* val dup */
    dictSdsKeyCompare,      /* key compare */
    dictSdsDestructor,      /* key destructor */
    engineFunctionDispose,  /* val destructor */
    NULL,                   /* allow to expand */
    NULL                    /* privdata */
};

dictType librariesDictType = {
    dictSdsHash,            /* hash function */
    NULL,                   /* key dup */
    NULL,                   /* val dup */
    dictSdsKeyCompare,      /* key compare */
    dictSdsDestructor,      /* key destructor */
    engineLibraryDispose,   /* val destructor */
    NULL,                   /* allow to expand */
    NULL                    /* privdata */
};

/* Memory sizing functions */
static size_t functionMallocSize(functionInfo *fi) {
    return zmalloc_size(fi) + sdsZmallocSize(fi->name)
            + (fi->desc ? sdsZmallocSize(fi->desc) : 0)
            + fi->li->ei->eng->get_function_memory_overhead(fi->function);
}

static size_t libraryMallocSize(functionLibInfo *li) {
    return zmalloc_size(li) + sdsZmallocSize(li->name)
            + sdsZmallocSize(li->code);
}

/* Dispose functions - KeyDB uses (void *privdata, void *obj) signature */
static void engineStatsDispose(void *privdata, void *obj) {
    UNUSED(privdata);
    functionsLibEngineStats *stats = (functionsLibEngineStats *)obj;
    zfree(stats);
}

static void engineFunctionDispose(void *privdata, void *obj) {
    UNUSED(privdata);
    if (!obj) return;
    
    functionInfo *fi = (functionInfo *)obj;
    sdsfree(fi->name);
    if (fi->desc) {
        sdsfree(fi->desc);
    }
    engine *eng = fi->li->ei->eng;
    eng->free_function(eng->engine_ctx, fi->function);
    zfree(fi);
}

static void engineLibraryFree(functionLibInfo *li) {
    if (!li) return;
    
    dictRelease(li->functions);
    sdsfree(li->name);
    sdsfree(li->code);
    zfree(li);
}

static void engineLibraryDispose(void *privdata, void *obj) {
    UNUSED(privdata);
    engineLibraryFree((functionLibInfo *)obj);
}

static void engineDispose(void *privdata, void *obj) {
    UNUSED(privdata);
    engineInfo *ei = (engineInfo *)obj;
    freeClient(ei->c);
    sdsfree(ei->name);
    ei->eng->free_ctx(ei->eng->engine_ctx);
    zfree(ei->eng);
    zfree(ei);
}

/* Verify function/library name is valid */
static int functionsVerifyName(sds name) {
    if (sdslen(name) == 0) {
        return C_ERR;
    }
    
    for (size_t i = 0; i < sdslen(name); i++) {
        char c = name[i];
        if (!isalnum(c) && c != '_') {
            return C_ERR;
        }
    }
    return C_OK;
}

/* Clear all functions from library context */
void functionsLibCtxClear(functionsLibCtx *lib_ctx) {
    dictEmpty(lib_ctx->functions, NULL);
    dictEmpty(lib_ctx->libraries, NULL);
    
    dictIterator *iter = dictGetIterator(lib_ctx->engines_stats);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
        functionsLibEngineStats *stats = (functionsLibEngineStats *)dictGetVal(entry);
        stats->n_functions = 0;
        stats->n_lib = 0;
    }
    dictReleaseIterator(iter);
    
    lib_ctx->cache_memory = 0;
}

/* Clear current library context */
void functionsLibCtxClearCurrent(int async) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    
    if (!curr_functions_lib_ctx) return;
    
    /* Just clear the contents, don't reinitialize */
    functionsLibCtxClear(curr_functions_lib_ctx);
    
    /* TODO: Implement async cleanup if needed */
    UNUSED(async);
}

/* Free library context */
void functionsLibCtxFree(functionsLibCtx *lib_ctx) {
    if (!lib_ctx) return;
    
    functionsLibCtxClear(lib_ctx);
    dictRelease(lib_ctx->functions);
    dictRelease(lib_ctx->libraries);
    dictRelease(lib_ctx->engines_stats);
    zfree(lib_ctx);
}

/* Create new library context */
functionsLibCtx* functionsLibCtxCreate(void) {
    functionsLibCtx *lib_ctx = (functionsLibCtx *)zmalloc(sizeof(*lib_ctx));
    lib_ctx->libraries = dictCreate(&librariesDictType, NULL);
    lib_ctx->functions = dictCreate(&functionDictType, NULL);
    lib_ctx->engines_stats = dictCreate(&engineStatsDictType, NULL);
    lib_ctx->cache_memory = 0;
    
    return lib_ctx;
}

/* Get current library context */
functionsLibCtx* functionsLibCtxGetCurrent(void) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    return curr_functions_lib_ctx;
}

/* Swap library context with current */
void functionsLibCtxSwapWithCurrent(functionsLibCtx *lib_ctx) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    curr_functions_lib_ctx = lib_ctx;
}

/* Get libraries dict */
dict* functionsLibGet(void) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    if (!curr_functions_lib_ctx) return NULL;
    return curr_functions_lib_ctx->libraries;
}

/* Get total functions memory */
unsigned long functionsMemory(void) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    if (!curr_functions_lib_ctx) return 0;
    return curr_functions_lib_ctx->cache_memory;
}

/* Get number of functions */
unsigned long functionsNum(void) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    if (!curr_functions_lib_ctx) return 0;
    return dictSize(curr_functions_lib_ctx->functions);
}

/* Get number of libraries */
unsigned long functionsLibNum(void) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    if (!curr_functions_lib_ctx) return 0;
    return dictSize(curr_functions_lib_ctx->libraries);
}

/* Register an engine */
int functionsRegisterEngine(const char *engine_name, engine *eng) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    
    sds engine_sds = sdsnew(engine_name);
    if (dictFetchValue(engines, engine_sds)) {
        sdsfree(engine_sds);
        return C_ERR;  /* Engine already registered */
    }
    
    engineInfo *ei = (engineInfo *)zmalloc(sizeof(*ei));
    ei->name = engine_sds;
    ei->eng = eng;
    ei->c = createClient(NULL, 0);  /* KeyDB: per-thread client */
    ei->c->flags |= CLIENT_LUA;  /* KeyDB uses CLIENT_LUA for scripts */
    
    dictAdd(engines, engine_sds, ei);
    
    /* Add engine stats */
    functionsLibEngineStats *stats = (functionsLibEngineStats *)zmalloc(sizeof(*stats));
    stats->n_lib = 0;
    stats->n_functions = 0;
    dictAdd(curr_functions_lib_ctx->engines_stats, sdsdup(engine_sds), stats);
    
    return C_OK;
}

/* Create a function in a library */
int functionLibCreateFunction(sds name, void *function, functionLibInfo *li, 
                               sds desc, uint64_t f_flags, sds *err) {
    if (functionsVerifyName(name) != C_OK) {
        *err = sdsnew("Function names can only contain letters, numbers, or underscores(_) and must be at least one character long");
        return C_ERR;
    }
    
    if (dictFetchValue(li->functions, name)) {
        *err = sdsnew("Function already exists in the library");
        return C_ERR;
    }
    
    functionInfo *fi = (functionInfo *)zmalloc(sizeof(*fi));
    fi->name = name;
    fi->function = function;
    fi->li = li;
    fi->desc = desc;
    fi->f_flags = f_flags;
    
    int res = dictAdd(li->functions, fi->name, fi);
    serverAssert(res == DICT_OK);

    /* Also register in the global functions dict for FCALL lookup */
    if (curr_functions_lib_ctx) {
        dictAdd(curr_functions_lib_ctx->functions, sdsdup(fi->name), fi);
    }

    return C_OK;
}

/* Initialize functions system */
int functionsInit(void) {
    engines = dictCreate(&engineDictType, NULL);
    curr_functions_lib_ctx = functionsLibCtxCreate();
    
    /* Register Lua engine */
    return luaEngineInitEngine();
}



/* ====================================================================
 * Phase 2: Lua Engine Implementation - Real Functions Support
 * Adapted from Redis 8.2.3 function_lua.c
 * ==================================================================== */

#define LUA_ENGINE_NAME "LUA"

/* Script flags - match Redis 8 definitions */
#define SCRIPT_FLAG_NO_WRITES (1ULL<<0)    /* Script can't write */
#define SCRIPT_FLAG_ALLOW_OOM (1ULL<<1)    /* Script can run on OOM */
#define SCRIPT_FLAG_ALLOW_STALE (1ULL<<2)  /* Script can run when replicas are stale */
#define SCRIPT_FLAG_NO_CLUSTER (1ULL<<3)   /* Script can't run in cluster mode */
#define SCRIPT_FLAG_ALLOW_CROSS_SLOT (1ULL<<4) /* Script can access cross-slot keys */

/* Lua engine context */
typedef struct luaEngineCtx {
    lua_State *lua;
} luaEngineCtx;

/* Lua function context */
typedef struct luaFunctionCtx {
    int lua_function_ref;  /* Lua registry reference */
} luaFunctionCtx;

/* Thread-local pointer to the library currently being loaded, used by luaRegisterFunction */
static thread_local functionLibInfo *tl_current_lib = nullptr;

/* Lua C function implementing redis.register_function() */
static int luaRegisterFunction(lua_State *L) {
    const char *name = NULL;
    int func_ref = LUA_NOREF;
    sds desc = NULL;
    uint64_t flags = 0;

    if (lua_isstring(L, 1) && lua_isfunction(L, 2)) {
        /* Simple form: redis.register_function("name", callback) */
        name = lua_tostring(L, 1);
        lua_pushvalue(L, 2);
        func_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else if (lua_istable(L, 1)) {
        /* Table form: redis.register_function{function_name="name", callback=func, ...} */
        lua_getfield(L, 1, "function_name");
        name = lua_tostring(L, -1);
        lua_pop(L, 1);

        lua_getfield(L, 1, "callback");
        if (lua_isfunction(L, -1)) {
            func_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        } else {
            lua_pop(L, 1);
        }

        lua_getfield(L, 1, "description");
        if (lua_isstring(L, -1)) {
            desc = sdsnew(lua_tostring(L, -1));
        }
        lua_pop(L, 1);

        /* Parse flags */
        lua_getfield(L, 1, "flags");
        if (lua_istable(L, -1)) {
            lua_pushnil(L);
            while (lua_next(L, -2)) {
                if (lua_isstring(L, -1)) {
                    const char *flag = lua_tostring(L, -1);
                    if (strcasecmp(flag, "no-writes") == 0) flags |= SCRIPT_FLAG_NO_WRITES;
                    else if (strcasecmp(flag, "allow-oom") == 0) flags |= SCRIPT_FLAG_ALLOW_OOM;
                    else if (strcasecmp(flag, "allow-stale") == 0) flags |= SCRIPT_FLAG_ALLOW_STALE;
                    else if (strcasecmp(flag, "no-cluster") == 0) flags |= SCRIPT_FLAG_NO_CLUSTER;
                    else if (strcasecmp(flag, "allow-cross-slot-keys") == 0) flags |= SCRIPT_FLAG_ALLOW_CROSS_SLOT;
                }
                lua_pop(L, 1);
            }
        }
        lua_pop(L, 1);
    } else {
        return luaL_error(L, "redis.register_function requires (name, callback) or {function_name=..., callback=...}");
    }

    if (!name || func_ref == LUA_NOREF) {
        return luaL_error(L, "redis.register_function: name and callback are required");
    }

    if (!tl_current_lib) {
        luaL_unref(L, LUA_REGISTRYINDEX, func_ref);
        return luaL_error(L, "redis.register_function can only be called during FUNCTION LOAD");
    }

    luaFunctionCtx *f_ctx = (luaFunctionCtx *)zmalloc(sizeof(*f_ctx));
    f_ctx->lua_function_ref = func_ref;

    sds err = NULL;
    sds sds_name = sdsnew(name);
    if (functionLibCreateFunction(sds_name, f_ctx, tl_current_lib, desc, flags, &err) != C_OK) {
        luaL_unref(L, LUA_REGISTRYINDEX, func_ref);
        zfree(f_ctx);
        sdsfree(sds_name);
        if (desc) sdsfree(desc);
        const char *e = err ? err : "Failed to register function";
        if (err) sdsfree(err);
        return luaL_error(L, "%s", e);
    }

    return 0;
}

/* Create a function library from Lua code */
static int luaEngineCreate(void *engine_ctx, functionLibInfo *li, sds code,
                            size_t timeout, sds *err) {
    UNUSED(timeout);

    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    lua_State *lua = lua_engine_ctx->lua;

    /* Skip the shebang line (#!...) - luaL_loadbuffer doesn't do this automatically */
    const char *lua_code = code;
    size_t lua_code_len = sdslen(code);
    if (lua_code_len >= 2 && lua_code[0] == '#' && lua_code[1] == '!') {
        const char *eol = (const char *)memchr(lua_code, '\n', lua_code_len);
        if (eol) {
            eol++; /* skip the newline */
            lua_code_len -= (eol - lua_code);
            lua_code = eol;
        }
    }

    /* Compile the Lua code */
    if (luaL_loadbuffer(lua, lua_code, lua_code_len, "@user_function")) {
        *err = sdscatprintf(sdsempty(), "Error compiling function: %s",
                           lua_tostring(lua, -1));
        lua_pop(lua, 1);
        return C_ERR;
    }

    /* Register redis.register_function before executing the library code */
    tl_current_lib = li;

    lua_getglobal(lua, "redis");
    if (!lua_istable(lua, -1)) {
        lua_pop(lua, 1);
        lua_newtable(lua);
        lua_setglobal(lua, "redis");
        lua_getglobal(lua, "redis");
    }
    lua_pushcfunction(lua, luaRegisterFunction);
    lua_setfield(lua, -2, "register_function");
    lua_pop(lua, 1); /* pop redis table */

    /* Execute the code to register functions */
    if (lua_pcall(lua, 0, 0, 0)) {
        *err = sdscatprintf(sdsempty(), "Error loading function: %s",
                           lua_tostring(lua, -1));
        lua_pop(lua, 1);
        tl_current_lib = nullptr;
        return C_ERR;
    }

    tl_current_lib = nullptr;
    return C_OK;
}

/* Call a Lua function - REAL implementation adapted from Redis 8 */
static void luaEngineCall(void *r_ctx, void *engine_ctx, void *compiled_function,
                          robj **keys, size_t nkeys, robj **args, size_t nargs) {
    client *c = (client *)r_ctx;

    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    lua_State *lua = lua_engine_ctx->lua;
    luaFunctionCtx *f_ctx = (luaFunctionCtx *)compiled_function;

    /* Set up Lua caller context so redis.call() works inside functions */
    client *prev_lua_caller = g_pserver->lua_caller;
    g_pserver->lua_caller = c;

    /* Select the correct DB in the Lua client */
    if (c && serverTL->lua_client) {
        selectDb(serverTL->lua_client, c->db->id);
    }

    /* Push the function from the registry onto the stack */
    lua_rawgeti(lua, LUA_REGISTRYINDEX, f_ctx->lua_function_ref);

    if (!lua_isfunction(lua, -1)) {
        lua_pop(lua, 1);
        if (c) addReplyError(c, "ERR Function reference invalid");
        serverLog(LL_WARNING, "Function reference invalid in luaEngineCall");
        return;
    }

    /* Push keys as Lua array */
    lua_newtable(lua);
    for (size_t i = 0; i < nkeys; i++) {
        lua_pushlstring(lua, (char*)ptrFromObj(keys[i]), sdslen((sds)ptrFromObj(keys[i])));
        lua_rawseti(lua, -2, i + 1);
    }

    /* Push args as Lua array */
    lua_newtable(lua);
    for (size_t i = 0; i < nargs; i++) {
        lua_pushlstring(lua, (char*)ptrFromObj(args[i]), sdslen((sds)ptrFromObj(args[i])));
        lua_rawseti(lua, -2, i + 1);
    }

    /* Call the function: function(KEYS, ARGV) */
    if (lua_pcall(lua, 2, 1, 0)) {
        const char *err_msg = lua_tostring(lua, -1);
        if (c) addReplyErrorFormat(c, "%s", err_msg ? err_msg : "unknown error");
        serverLog(LL_WARNING, "Error calling Lua function: %s", err_msg ? err_msg : "unknown");
        lua_pop(lua, 1);  /* Pop error */
        return;
    }

    /* Convert Lua return value to Redis reply */
    if (c) {
        luaReplyToRedisReply(c, lua); /* pops the value from the stack */
    } else {
        lua_pop(lua, 1);
    }

    /* Restore previous Lua caller context */
    g_pserver->lua_caller = prev_lua_caller;
}

/* Memory overhead functions */
static size_t luaEngineGetUsedMemory(void *engine_ctx) {
    UNUSED(engine_ctx);
    /* Return approximate Lua memory usage */
    return 0;  /* TODO: Implement proper memory tracking */
}

static size_t luaEngineFunctionMemoryOverhead(void *compiled_function) {
    luaFunctionCtx *f_ctx = (luaFunctionCtx *)compiled_function;
    return zmalloc_size(f_ctx);
}

static size_t luaEngineMemoryOverhead(void *engine_ctx) {
    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    return zmalloc_size(lua_engine_ctx);
}

/* Free a compiled function */
static void luaEngineFreeFunction(void *engine_ctx, void *compiled_function) {
    if (!compiled_function) return;
    
    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    luaFunctionCtx *f_ctx = (luaFunctionCtx *)compiled_function;
    
    /* Unreference from Lua registry (KeyDB uses lua_unref, not luaL_unref) */
    lua_unref(lua_engine_ctx->lua, f_ctx->lua_function_ref);
    zfree(f_ctx);
}

/* Free engine context */
static void luaEngineFreeCtx(void *engine_ctx) {
    if (!engine_ctx) return;
    
    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)engine_ctx;
    /* Note: We reuse KeyDB's global Lua state, so don't close it */
    zfree(lua_engine_ctx);
}

/* Initialize and register the Lua engine */
extern "C" int luaEngineInitEngine(void) {
    /* Create engine structure with callbacks */
    engine *lua_engine = (engine *)zmalloc(sizeof(*lua_engine));
    
    /* Create Lua engine context (reuse KeyDB's existing Lua state) */
    luaEngineCtx *lua_engine_ctx = (luaEngineCtx *)zmalloc(sizeof(*lua_engine_ctx));
    lua_engine_ctx->lua = g_pserver->lua;  /* Reuse KeyDB's global Lua state */
    
    /* Set up engine callbacks */
    lua_engine->engine_ctx = lua_engine_ctx;
    lua_engine->create = luaEngineCreate;
    lua_engine->call = luaEngineCall;
    lua_engine->get_used_memory = luaEngineGetUsedMemory;
    lua_engine->get_function_memory_overhead = luaEngineFunctionMemoryOverhead;
    lua_engine->get_engine_memory_overhead = luaEngineMemoryOverhead;
    lua_engine->free_function = luaEngineFreeFunction;
    lua_engine->free_ctx = luaEngineFreeCtx;
    
    /* Register the Lua engine with the functions system */
    if (functionsRegisterEngine(LUA_ENGINE_NAME, lua_engine) != C_OK) {
        serverLog(LL_WARNING, "Failed to register Lua engine for Functions API");
        zfree(lua_engine_ctx);
        zfree(lua_engine);
        return C_ERR;
    }
    
    serverLog(LL_NOTICE, "Lua engine registered for Redis Functions API");
    return C_OK;
}
/* ====================================================================
 * Phase 3: FUNCTION Command Implementation
 * ==================================================================== */

/* FUNCTION LOAD [REPLACE] <engine_name> <library_name> <code> */
static void functionLoadCommand(client *c) {
    int replace = 0;
    int argc_pos = 2;
    
    /* Check for REPLACE option */
    if (c->argc >= 3) {
        if (!strcasecmp((char*)ptrFromObj(c->argv[2]), "replace")) {
            replace = 1;
            argc_pos = 3;
        }
    }
    
    if (c->argc != argc_pos + 1) {
        addReplyError(c, "wrong number of arguments for 'function load' command");
        return;
    }
    
    sds code = (sds)ptrFromObj(c->argv[argc_pos]);
    
    /* Parse shebang line: #!<engine> name=<libname> */
    if (sdslen(code) < 5 || code[0] != '#' || code[1] != '!') {
        addReplyError(c, "library code must start with shebang statement");
        return;
    }
    
    /* Find end of first line */
    char *eol = strchr(code + 2, '\n');
    if (!eol) {
        addReplyError(c, "missing library metadata");
        return;
    }
    
    /* Extract shebang line */
    sds shebang = sdsnewlen(code + 2, eol - (code + 2));
    
    /* Parse engine name (before space or end of line) */
    char *space = strchr(shebang, ' ');
    sds engine_name = space ? sdsnewlen(shebang, space - shebang) : sdsdup(shebang);
    
    /* Parse library name from "name=<libname>" */
    sds library_name = NULL;
    if (space) {
        char *name_prefix = strstr(space + 1, "name=");
        if (name_prefix) {
            char *name_start = name_prefix + 5;
            char *name_end = name_start;
            while (*name_end && !isspace(*name_end)) name_end++;
            library_name = sdsnewlen(name_start, name_end - name_start);
        }
    }
    
    if (!library_name) {
        sdsfree(engine_name);
        sdsfree(shebang);
        addReplyError(c, "library name must be specified in shebang");
        return;
    }
    
    sdsfree(shebang);
    sds err = NULL;
    
    std::lock_guard<std::mutex> lock(functions_mutex);
    
    /* Check if engine exists */
    engineInfo *ei = (engineInfo *)dictFetchValue(engines, engine_name);
    if (!ei) {
        addReplyErrorFormat(c, "ERR unknown engine '%s'", engine_name);
        sdsfree(engine_name);
        sdsfree(library_name);
        return;
    }

    /* Check if library already exists */
    functionLibInfo *existing_li = (functionLibInfo *)dictFetchValue(curr_functions_lib_ctx->libraries, library_name);
    if (existing_li && !replace) {
        addReplyErrorFormat(c, "ERR Library '%s' already exists", library_name);
        sdsfree(engine_name);
        sdsfree(library_name);
        return;
    }
    
    /* Create new library info */
    functionLibInfo *li = (functionLibInfo *)zcalloc(sizeof(*li));
    li->name = sdsdup(library_name);
    li->ei = ei;
    li->code = sdsdup(code);
    li->functions = dictCreate(&libraryFunctionDictType, NULL);
    
    /* Call engine to create/compile the library */
    if (ei->eng->create(ei->eng->engine_ctx, li, code, LOAD_TIMEOUT_MS, &err) != C_OK) {
        addReplyErrorFormat(c, "%s", err ? err : "Failed to create library");
        if (err) sdsfree(err);
        dictRelease(li->functions);
        sdsfree(li->name);
        sdsfree(li->code);
        zfree(li);
        sdsfree(engine_name);
        sdsfree(library_name);
        return;
    }
    
    /* Remove old library if replacing */
    if (existing_li) {
        dictDelete(curr_functions_lib_ctx->libraries, library_name);
    }
    
    /* Register the library */
    dictAdd(curr_functions_lib_ctx->libraries, sdsdup(library_name), li);
    
    /* Update engine stats */
    functionsLibEngineStats *stats = (functionsLibEngineStats *)dictFetchValue(curr_functions_lib_ctx->engines_stats, ei->name);
    stats->n_lib++;
    stats->n_functions += dictSize(li->functions);

    addReplyBulkSds(c, sdsdup(library_name));

    sdsfree(engine_name);
    sdsfree(library_name);

    /* Replicate the command */
    g_pserver->dirty++;
}

/* FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE] */
static void functionListCommand(client *c) {
    int with_code = 0;
    sds library_name = NULL;
    
    /* Parse optional arguments */
    for (int i = 2; i < c->argc; i++) {
        sds arg = (sds)ptrFromObj(c->argv[i]);
        if (!strcasecmp(arg, "WITHCODE")) {
            with_code = 1;
        } else if (!strcasecmp(arg, "LIBRARYNAME") && i + 1 < c->argc) {
            library_name = (sds)ptrFromObj(c->argv[++i]);
        } else {
            addReplyErrorFormat(c, "ERR Unknown FUNCTION LIST option '%s'", arg);
            return;
        }
    }
    
    std::lock_guard<std::mutex> lock(functions_mutex);
    
    if (!curr_functions_lib_ctx || !curr_functions_lib_ctx->libraries) {
        addReplyArrayLen(c, 0);
        return;
    }
    
    /* Count matching libraries if pattern provided */
    size_t reply_len = 0;
    if (library_name) {
        /* Count matches first for deferred length */
        dictIterator *iter = dictGetIterator(curr_functions_lib_ctx->libraries);
        dictEntry *entry;
        while ((entry = dictNext(iter)) != NULL) {
            functionLibInfo *li = (functionLibInfo *)dictGetVal(entry);
            /* Simple pattern matching - exact or contains */
            if (strstr(li->name, library_name)) {
                reply_len++;
            }
        }
        dictReleaseIterator(iter);
        addReplyArrayLen(c, reply_len);
    } else {
        addReplyArrayLen(c, dictSize(curr_functions_lib_ctx->libraries));
    }
    
    /* Output libraries */
    dictIterator *iter = dictGetIterator(curr_functions_lib_ctx->libraries);
    dictEntry *entry;
    while ((entry = dictNext(iter)) != NULL) {
        functionLibInfo *li = (functionLibInfo *)dictGetVal(entry);
        
        /* Filter by pattern if provided */
        if (library_name && !strstr(li->name, library_name)) {
            continue;
        }
        
        addReplyMapLen(c, with_code ? 4 : 3);
        
        /* Library name */
        addReplyBulkCString(c, "library_name");
        addReplyBulkCBuffer(c, li->name, sdslen(li->name));
        
        /* Engine */
        addReplyBulkCString(c, "engine");
        addReplyBulkCBuffer(c, li->ei->name, sdslen(li->ei->name));
        
        /* Functions */
        addReplyBulkCString(c, "functions");
        addReplyArrayLen(c, dictSize(li->functions));
        dictIterator *func_iter = dictGetIterator(li->functions);
        dictEntry *func_entry;
        while ((func_entry = dictNext(func_iter)) != NULL) {
            functionInfo *fi = (functionInfo *)dictGetVal(func_entry);
            addReplyMapLen(c, 2);
            addReplyBulkCString(c, "name");
            addReplyBulkCBuffer(c, fi->name, sdslen(fi->name));
            addReplyBulkCString(c, "description");
            if (fi->desc) {
                addReplyBulkCBuffer(c, fi->desc, sdslen(fi->desc));
            } else {
                addReplyNull(c);
            }
        }
        dictReleaseIterator(func_iter);
        
        /* Code if requested */
        if (with_code) {
            addReplyBulkCString(c, "library_code");
            addReplyBulkCBuffer(c, li->code, sdslen(li->code));
        }
    }
    dictReleaseIterator(iter);
}

/* FUNCTION STATS */
static void functionStatsCommand(client *c) {
    std::lock_guard<std::mutex> lock(functions_mutex);
    
    addReplyMapLen(c, 2);
    
    /* running_script */
    addReplyBulkCString(c, "running_script");
    addReplyNull(c);  /* TODO: Track running functions */
    
    /* engines */
    addReplyBulkCString(c, "engines");
    
    if (!engines || !curr_functions_lib_ctx || !curr_functions_lib_ctx->engines_stats) {
        addReplyMapLen(c, 0);
        return;
    }
    
    addReplyMapLen(c, dictSize(engines));
    
    dictIterator *iter = dictGetIterator(engines);
    dictEntry *entry;
    while ((entry = dictNext(iter)) != NULL) {
        engineInfo *ei = (engineInfo *)dictGetVal(entry);
        if (!ei || !ei->name) continue;
        
        functionsLibEngineStats *stats = (functionsLibEngineStats *)dictFetchValue(curr_functions_lib_ctx->engines_stats, ei->name);
        if (!stats) continue;
        
        addReplyBulkCBuffer(c, ei->name, sdslen(ei->name));
        addReplyMapLen(c, 2);
        addReplyBulkCString(c, "libraries_count");
        addReplyLongLong(c, stats->n_lib);
        addReplyBulkCString(c, "functions_count");
        addReplyLongLong(c, stats->n_functions);
    }
    dictReleaseIterator(iter);
}

/* FUNCTION FLUSH [ASYNC | SYNC] */
static void functionFlushCommand(client *c) {
    int async = 0;
    
    if (c->argc == 3) {
        char *mode = (char*)ptrFromObj(c->argv[2]);
        if (!strcasecmp(mode, "sync")) {
            async = 0;
        } else if (!strcasecmp(mode, "async")) {
            async = 1;
        } else {
            addReplyError(c, "FUNCTION FLUSH only supports SYNC|ASYNC option");
            return;
        }
    }
    
    std::lock_guard<std::mutex> lock(functions_mutex);

    if (curr_functions_lib_ctx) {
        functionsLibCtxClear(curr_functions_lib_ctx);
    }
    
    addReply(c, shared.ok);
    g_pserver->dirty++;
}

/* Main FUNCTION command router */
void functionCommand(client *c) {
    if (c->argc < 2) {
        addReplyError(c, "ERR wrong number of arguments for 'function' command");
        return;
    }

    char *subcommand = (char*)ptrFromObj(c->argv[1]);

    if (!strcasecmp(subcommand, "LOAD")) {
        functionLoadCommand(c);
    } else if (!strcasecmp(subcommand, "LIST")) {
        functionListCommand(c);
    } else if (!strcasecmp(subcommand, "STATS")) {
        functionStatsCommand(c);
    } else if (!strcasecmp(subcommand, "FLUSH")) {
        functionFlushCommand(c);
    } else if (!strcasecmp(subcommand, "DELETE")) {
        if (c->argc != 3) {
            addReplyError(c, "wrong number of arguments for 'function delete' command");
            return;
        }
        sds library_name = (sds)ptrFromObj(c->argv[2]);
        
        std::lock_guard<std::mutex> lock(functions_mutex);
        
        if (!curr_functions_lib_ctx || !curr_functions_lib_ctx->libraries) {
            addReplyError(c, "ERR Library not found");
            return;
        }
        
        functionLibInfo *li = (functionLibInfo *)dictFetchValue(curr_functions_lib_ctx->libraries, library_name);
        if (!li) {
            addReplyError(c, "ERR Library not found");
            return;
        }
        
        /* Delete all functions in the library */
        dictIterator *iter = dictGetIterator(li->functions);
        dictEntry *entry;
        while ((entry = dictNext(iter)) != NULL) {
            functionInfo *fi = (functionInfo *)dictGetVal(entry);
            dictDelete(curr_functions_lib_ctx->functions, fi->name);
        }
        dictReleaseIterator(iter);
        
        /* Update engine stats */
        functionsLibEngineStats *stats = (functionsLibEngineStats *)dictFetchValue(curr_functions_lib_ctx->engines_stats, li->ei->name);
        if (stats) {
            stats->n_lib--;
            stats->n_functions -= dictSize(li->functions);
        }
        
        /* Delete the library */
        dictDelete(curr_functions_lib_ctx->libraries, library_name);
        
        addReply(c, shared.ok);
        g_pserver->dirty++;
    } else if (!strcasecmp(subcommand, "DUMP")) {
        /* Simple DUMP - return serialized libraries (simplified version) */
        std::lock_guard<std::mutex> lock(functions_mutex);
        
        sds payload = sdsempty();
        
        if (curr_functions_lib_ctx && curr_functions_lib_ctx->libraries) {
            dictIterator *iter = dictGetIterator(curr_functions_lib_ctx->libraries);
            dictEntry *entry;
            while ((entry = dictNext(iter)) != NULL) {
                functionLibInfo *li = (functionLibInfo *)dictGetVal(entry);
                /* Format: engine_name\nlib_name\ncode\n--- */
                payload = sdscatprintf(payload, "%s\n%s\n%s\n---\n", 
                                      li->ei->name, li->name, li->code);
            }
            dictReleaseIterator(iter);
        }
        
        addReplyBulkSds(c, payload);
    } else if (!strcasecmp(subcommand, "RESTORE")) {
        if (c->argc < 3) {
            addReplyError(c, "wrong number of arguments for 'function restore' command");
            return;
        }
        
        sds payload = (sds)ptrFromObj(c->argv[2]);
        int replace = 0;
        
        /* Check for REPLACE/APPEND/FLUSH policy */
        if (c->argc >= 4) {
            sds policy = (sds)ptrFromObj(c->argv[3]);
            if (!strcasecmp(policy, "REPLACE")) {
                replace = 1;
            } else if (!strcasecmp(policy, "FLUSH")) {
                functionsLibCtxClearCurrent(0);
            }
        }
        
        /* Parse and restore libraries from payload */
        int count;
        sds *lines = sdssplitlen(payload, sdslen(payload), "\n", 1, &count);
        int i = 0;
        int restored = 0;

        std::lock_guard<std::mutex> lock(functions_mutex);

        while (i + 2 < count) {
            sds engine_name = lines[i++];
            sds lib_name = lines[i++];
            sds code = lines[i++];

            /* Skip separator */
            if (i < count && strcmp(lines[i], "---") == 0) {
                i++;
            }
            
            engineInfo *ei = (engineInfo *)dictFetchValue(engines, engine_name);
            if (!ei) continue;
            
            functionLibInfo *existing = (functionLibInfo *)dictFetchValue(curr_functions_lib_ctx->libraries, lib_name);
            if (existing && !replace) continue;
            
            functionLibInfo *li = (functionLibInfo *)zcalloc(sizeof(*li));
            li->name = sdsdup(lib_name);
            li->ei = ei;
            li->code = sdsdup(code);
            li->functions = dictCreate(&libraryFunctionDictType, NULL);
            
            sds err = NULL;
            if (ei->eng->create(ei->eng->engine_ctx, li, code, LOAD_TIMEOUT_MS, &err) == C_OK) {
                if (existing) {
                    dictDelete(curr_functions_lib_ctx->libraries, lib_name);
                }
                dictAdd(curr_functions_lib_ctx->libraries, sdsdup(lib_name), li);
                restored++;
            } else {
                if (err) sdsfree(err);
                dictRelease(li->functions);
                sdsfree(li->name);
                sdsfree(li->code);
                zfree(li);
            }
        }
        
        sdsfreesplitres(lines, count);
        addReply(c, shared.ok);
        g_pserver->dirty++;
    } else if (!strcasecmp(subcommand, "KILL")) {
        /* FUNCTION KILL - would kill running function, but we don't track that yet */
        addReplyError(c, "No scripts in execution right now");
    } else {
        addReplyErrorFormat(c, "ERR unknown FUNCTION subcommand '%s'", subcommand);
    }
}

/* ====================================================================
 * Phase 4: FCALL / FCALL_RO Implementation
 * ==================================================================== */

/* Generic FCALL implementation */
static void fcallCommandGeneric(client *c, int ro) {
    if (c->argc < 3) {
        addReplyError(c, "ERR wrong number of arguments for FCALL");
        return;
    }
    
    sds function_name = (sds)ptrFromObj(c->argv[1]);
    long long numkeys;
    
    /* Get number of keys */
    if (getLongLongFromObjectOrReply(c, c->argv[2], &numkeys, NULL) != C_OK) {
        return;
    }
    
    if (numkeys < 0) {
        addReplyError(c, "ERR Number of keys can't be negative");
        return;
    }
    
    if (numkeys > (c->argc - 3)) {
        addReplyError(c, "ERR Number of keys can't be greater than number of args");
        return;
    }
    
    std::lock_guard<std::mutex> lock(functions_mutex);
    
    /* Check if Functions system is initialized */
    if (!curr_functions_lib_ctx || !curr_functions_lib_ctx->functions) {
        addReplyErrorFormat(c, "ERR Function '%s' not found", function_name);
        return;
    }
    
    /* Find the function */
    functionInfo *fi = (functionInfo *)dictFetchValue(curr_functions_lib_ctx->functions, function_name);
    if (!fi) {
        addReplyErrorFormat(c, "ERR Function '%s' not found", function_name);
        return;
    }
    
    /* Validate function structure */
    if (!fi->li || !fi->li->ei || !fi->li->ei->eng || !fi->function) {
        addReplyError(c, "ERR Function library is invalid");
        return;
    }
    
    /* Check read-only constraint */
    if (ro && !(fi->f_flags & SCRIPT_FLAG_NO_WRITES)) {
        addReplyError(c, "ERR Can not execute a function with write flag using fcall_ro");
        return;
    }
    
    /* Get keys and args */
    robj **keys = (numkeys > 0) ? c->argv + 3 : NULL;
    robj **args = (c->argc - 3 - numkeys > 0) ? c->argv + 3 + numkeys : NULL;
    size_t nargs = c->argc - 3 - numkeys;
    
    /* Call the function - pass client as r_ctx so luaEngineCall can send the reply */
    engine *eng = fi->li->ei->eng;
    eng->call(c, eng->engine_ctx, fi->function, keys, (size_t)numkeys, args, nargs);
    
    /* Replicate write functions */
    if (!ro) {
        g_pserver->dirty++;
    }
}

/* FCALL <FUNCTION NAME> numkeys key [key ...] arg [arg ...] */
void fcallCommand(client *c) {
    fcallCommandGeneric(c, 0);
}

/* FCALL_RO <FUNCTION NAME> numkeys key [key ...] arg [arg ...] */
void fcallroCommand(client *c) {
    fcallCommandGeneric(c, 1);
}

