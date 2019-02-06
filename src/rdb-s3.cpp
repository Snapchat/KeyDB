extern "C" {
#include "rio.h"
#include "server.h"
}

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
extern "C" int rdbSaveS3(char *s3bucket, rdbSaveInfo *rsi)
{
    (void)s3bucket;
    (void)rsi;
    // NOP
    return C_ERR;
}