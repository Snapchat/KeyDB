/*
 * Functions API for KeyDB - Real Implementation
 * Ported from Redis 8.2.3 functions.h
 */

#ifndef __KEYDB_FUNCTIONS_H
#define __KEYDB_FUNCTIONS_H

#include "server.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
typedef struct functionsLibCtx functionsLibCtx;
typedef struct functionLibInfo functionLibInfo;
typedef struct functionInfo functionInfo;
typedef struct engineInfo engineInfo;

/* Engine callbacks */
typedef struct engine {
    void *engine_ctx;
    
    /* Create function from code */
    int (*create)(void *engine_ctx, functionLibInfo *li, sds code, size_t timeout, sds *err);
    
    /* Call function */
    void (*call)(void *r_ctx, void *engine_ctx, void *compiled_function,
                 robj **keys, size_t nkeys, robj **args, size_t nargs);
    
    /* Memory functions */
    size_t (*get_used_memory)(void *engine_ctx);
    size_t (*get_function_memory_overhead)(void *compiled_function);
    size_t (*get_engine_memory_overhead)(void *engine_ctx);
    
    /* Cleanup */
    void (*free_function)(void *engine_ctx, void *compiled_function);
    void (*free_ctx)(void *engine_ctx);
} engine;

/* Engine info */
struct engineInfo {
    sds name;
    engine *eng;  /* Changed from 'engine' to avoid name collision */
    client *c;
};

/* Function info */
struct functionInfo {
    sds name;
    void *function;          /* Compiled function (engine-specific) */
    functionLibInfo *li;     /* Parent library */
    sds desc;                /* Description */
    uint64_t f_flags;        /* Flags */
};

/* Library info */
struct functionLibInfo {
    sds name;
    dict *functions;
    engineInfo *ei;
    sds code;
};

/* Library context - holds all libraries and functions */
struct functionsLibCtx {
    dict *libraries;         /* Library name -> functionLibInfo */
    dict *functions;         /* Function name -> functionInfo */
    size_t cache_memory;     /* Memory used */
    dict *engines_stats;     /* Per-engine statistics */
};

/* API functions */
int functionsInit(void);
functionsLibCtx* functionsLibCtxGetCurrent(void);
functionsLibCtx* functionsLibCtxCreate(void);
void functionsLibCtxFree(functionsLibCtx *lib_ctx);
void functionsLibCtxSwapWithCurrent(functionsLibCtx *lib_ctx);
void functionsLibCtxClear(functionsLibCtx *lib_ctx);
void functionsLibCtxClearCurrent(int async);

int functionsRegisterEngine(const char *engine_name, engine *eng);
int functionLibCreateFunction(sds name, void *function, functionLibInfo *li, 
                               sds desc, uint64_t f_flags, sds *err);

sds functionsCreateWithLibraryCtx(sds code, int replace, sds *err,
                                   functionsLibCtx *lib_ctx, size_t timeout);

dict* functionsLibGet(void);
unsigned long functionsMemory(void);
unsigned long functionsNum(void);
unsigned long functionsLibNum(void);

/* Lua engine */
int luaEngineInitEngine(void);

#ifdef __cplusplus
}  /* End extern "C" */
#endif

/* Command functions - declared in server.h with C++ linkage, implemented in functions.cpp */
/* These are NOT in extern "C" block - they use C++ linkage */

#endif /* __KEYDB_FUNCTIONS_H */

