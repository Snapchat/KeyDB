/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "lzf.h"    /* LZF compression library */
#include "zipmap.h"
#include "endianconv.h"
#include "stream.h"
#include "storage.h"
#include "cron.h"

#include <math.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/param.h>

/* This macro is called when the internal RDB structure is corrupt */
#define rdbReportCorruptRDB(...) rdbReportError(1, __LINE__,__VA_ARGS__)
/* This macro is called when RDB read failed (possibly a short read) */
#define rdbReportReadError(...) rdbReportError(0, __LINE__,__VA_ARGS__)

const char* rdbFileBeingLoaded = NULL; /* used for rdb checking on read error */
extern int rdbCheckMode;
void rdbCheckError(const char *fmt, ...);
void rdbCheckSetError(const char *fmt, ...);

#ifdef __GNUC__
void rdbReportError(int corruption_error, int linenum, const char *reason, ...) __attribute__ ((format (printf, 3, 4)));
#endif
void rdbReportError(int corruption_error, int linenum, const char *reason, ...) {
    va_list ap;
    char msg[1024];
    int len;

    len = snprintf(msg,sizeof(msg),
        "Internal error in RDB reading offset %llu, function at rdb.c:%d -> ",
        (unsigned long long)g_pserver->loading_loaded_bytes, linenum);
    va_start(ap,reason);
    vsnprintf(msg+len,sizeof(msg)-len,reason,ap);
    va_end(ap);

    if (!g_pserver->loading) {
        /* If we're in the context of a RESTORE command, just propagate the error. */
        /* log in VERBOSE, and return (don't exit). */
        serverLog(LL_VERBOSE, "%s", msg);
        return;
    } else if (rdbCheckMode) {
        /* If we're inside the rdb checker, let it handle the error. */
        rdbCheckError("%s",msg);
    } else if (rdbFileBeingLoaded) {
        /* If we're loading an rdb file form disk, run rdb check (and exit) */
        serverLog(LL_WARNING, "%s", msg);
        const char *argv[2] = {"",rdbFileBeingLoaded};
        redis_check_rdb_main(2,argv,NULL);
    } else if (corruption_error) {
        /* In diskless loading, in case of corrupt file, log and exit. */
        serverLog(LL_WARNING, "%s. Failure loading rdb format", msg);
    } else {
        /* In diskless loading, in case of a short read (not a corrupt
         * file), log and proceed (don't exit). */
        serverLog(LL_WARNING, "%s. Failure loading rdb format from socket, assuming connection error, resuming operation.", msg);
        return;
    }
    serverLog(LL_WARNING, "Terminating server after rdb file reading failure.");
    exit(1);
}

static ssize_t rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

int rdbSaveType(rio *rdb, unsigned char type) {
    return rdbWriteRaw(rdb,&type,1);
}

/* Load a "type" in RDB format, that is a one byte unsigned integer.
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, the EXPIRE type, and so forth. */
int rdbLoadType(rio *rdb) {
    unsigned char type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}

/* This is only used to load old databases stored with the RDB_OPCODE_EXPIRETIME
 * opcode. New versions of Redis store using the RDB_OPCODE_EXPIRETIME_MS
 * opcode. On error -1 is returned, however this could be a valid time, so
 * to check for loading errors the caller should call rioGetReadError() after
 * calling this function. */
time_t rdbLoadTime(rio *rdb) {
    int32_t t32;
    if (rioRead(rdb,&t32,4) == 0) return -1;
    return (time_t)t32;
}

int rdbSaveMillisecondTime(rio *rdb, long long t) {
    int64_t t64 = (int64_t) t;
    memrev64ifbe(&t64); /* Store in little endian. */
    return rdbWriteRaw(rdb,&t64,8);
}

/* This function loads a time from the RDB file. It gets the version of the
 * RDB because, unfortunately, before Redis 5 (RDB version 9), the function
 * failed to convert data to/from little endian, so RDB files with keys having
 * expires could not be shared between big endian and little endian systems
 * (because the expire time will be totally wrong). The fix for this is just
 * to call memrev64ifbe(), however if we fix this for all the RDB versions,
 * this call will introduce an incompatibility for big endian systems:
 * after upgrading to Redis version 5 they will no longer be able to load their
 * own old RDB files. Because of that, we instead fix the function only for new
 * RDB versions, and load older RDB versions as we used to do in the past,
 * allowing big endian systems to load their own old RDB files.
 *
 * On I/O error the function returns LLONG_MAX, however if this is also a
 * valid stored value, the caller should use rioGetReadError() to check for
 * errors after calling this function. */
long long rdbLoadMillisecondTime(rio *rdb, int rdbver) {
    int64_t t64;
    if (rioRead(rdb,&t64,8) == 0) return LLONG_MAX;
    if (rdbver >= 9) /* Check the top comment of this function. */
        memrev64ifbe(&t64); /* Convert in big endian if the system is BE. */
    return (long long)t64;
}

/* Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the RDB_* definitions for more information
 * on the types of encoding. */
int rdbSaveLen(rio *rdb, uint64_t len) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(RDB_6BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        uint32_t len32 = htonl(len);
        if (rdbWriteRaw(rdb,&len32,4) == -1) return -1;
        nwritten = 1+4;
    } else {
        /* Save a 64 bit len */
        buf[0] = RDB_64BITLEN;
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonu64(len);
        if (rdbWriteRaw(rdb,&len,8) == -1) return -1;
        nwritten = 1+8;
    }
    return nwritten;
}


/* Load an encoded length. If the loaded length is a normal length as stored
 * with rdbSaveLen(), the read length is set to '*lenptr'. If instead the
 * loaded length describes a special encoding that follows, then '*isencoded'
 * is set to 1 and the encoding format is stored at '*lenptr'.
 *
 * See the RDB_ENC_* definitions in rdb.h for more information on special
 * encodings.
 *
 * The function returns -1 on error, 0 on success. */
int rdbLoadLenByRef(rio *rdb, int *isencoded, uint64_t *lenptr) {
    unsigned char buf[2];
    int type;

    if (isencoded) *isencoded = 0;
    if (rioRead(rdb,buf,1) == 0) return -1;
    type = (buf[0]&0xC0)>>6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        *lenptr = buf[0]&0x3F;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        *lenptr = buf[0]&0x3F;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(rdb,buf+1,1) == 0) return -1;
        *lenptr = ((buf[0]&0x3F)<<8)|buf[1];
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
        if (rioRead(rdb,&len,4) == 0) return -1;
        *lenptr = ntohl(len);
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
        if (rioRead(rdb,&len,8) == 0) return -1;
        *lenptr = ntohu64(len);
    } else {
        rdbReportCorruptRDB(
            "Unknown length encoding %d in rdbLoadLen()",type);
        return -1; /* Never reached. */
    }
    return 0;
}

/* This is like rdbLoadLenByRef() but directly returns the value read
 * from the RDB stream, signaling an error by returning RDB_LENERR
 * (since it is a too large count to be applicable in any Redis data
 * structure). */
uint64_t rdbLoadLen(rio *rdb, int *isencoded) {
    uint64_t len;

    if (rdbLoadLenByRef(rdb,isencoded,&len) == -1) return RDB_LENERR;
    return len;
}

/* Encodes the "value" argument as integer when it fits in the supported ranges
 * for encoded types. If the function successfully encodes the integer, the
 * representation is stored in the buffer pointer to by "enc" and the string
 * length is returned. Otherwise 0 is returned. */
int rdbEncodeInteger(long long value, unsigned char *enc) {
    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

/* Loads an integer-encoded object with the specified encoding type "enctype".
 * The returned value changes according to the flags, see
 * rdbGenericLoadStringObject() for more info. */
void *rdbLoadIntegerObject(rio *rdb, int enctype, int flags, size_t *lenptr) {
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int encode = flags & RDB_LOAD_ENC;
    unsigned char enc[4];
    long long val;

    if (enctype == RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        rdbReportCorruptRDB("Unknown RDB integer encoding type %d",enctype);
        return NULL; /* Never reached. */
    }
    if (plain || sds) {
        char buf[LONG_STR_SIZE], *p;
        int len = ll2string(buf,sizeof(buf),val);
        if (lenptr) *lenptr = len;
        p = (char*)(plain ? zmalloc(len, MALLOC_SHARED) : sdsnewlen(SDS_NOINIT,len));
        memcpy(p,buf,len);
        return p;
    } else if (encode) {
        return createStringObjectFromLongLongForValue(val);
    } else {
        return createObject(OBJ_STRING,sdsfromlonglong(val));
    }
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space */
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0') return 0;
    ll2string(buf,32,value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (strlen(buf) != len || memcmp(buf,s,len)) return 0;

    return rdbEncodeInteger(value,enc);
}

ssize_t rdbSaveLzfBlob(rio *rdb, void *data, size_t compress_len,
                       size_t original_len) {
    unsigned char byte;
    ssize_t n, nwritten = 0;

    /* Data compressed! Let's save it on disk */
    byte = (RDB_ENCVAL<<6)|RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,compress_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,original_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbWriteRaw(rdb,data,compress_len)) == -1) goto writeerr;
    nwritten += n;

    return nwritten;

writeerr:
    return -1;
}

ssize_t rdbSaveLzfStringObject(rio *rdb, const unsigned char *s, size_t len) {
    size_t comprlen, outlen;
    void *out;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= 4) return 0;
    outlen = len-4;
    if ((out = zmalloc(outlen+1, MALLOC_LOCAL)) == NULL) return 0;
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }
    ssize_t nwritten = rdbSaveLzfBlob(rdb, out, comprlen, len);
    zfree(out);
    return nwritten;
}

/* Load an LZF compressed string in RDB format. The returned value
 * changes according to 'flags'. For more info check the
 * rdbGenericLoadStringObject() function. */
void *rdbLoadLzfStringObject(rio *rdb, int flags, size_t *lenptr) {
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    uint64_t len, clen;
    unsigned char *c = NULL;
    char *val = NULL;

    if ((clen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((c = (unsigned char*)ztrymalloc(clen)) == NULL) {
        serverLog(g_pserver->loading? LL_WARNING: LL_VERBOSE, "rdbLoadLzfStringObject failed allocating %llu bytes", (unsigned long long)clen);
        goto err;
    }

    /* Allocate our target according to the uncompressed size. */
    if (plain) {
        val = (char*)ztrymalloc(len);
    } else {
        val = sdstrynewlen(SDS_NOINIT,len);
    }
    if (!val) {
        serverLog(g_pserver->loading? LL_WARNING: LL_VERBOSE, "rdbLoadLzfStringObject failed allocating %llu bytes", (unsigned long long)len);
        goto err;
    }

    if (lenptr) *lenptr = len;

    /* Load the compressed representation and uncompress it to target. */
    if (rioRead(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) != len) {
        rdbReportCorruptRDB("Invalid LZF compressed string");
        goto err;
    }
    zfree(c);

    if (plain || sds) {
        return val;
    } else {
        return createObject(OBJ_STRING,val);
    }
err:
    zfree(c);
    if (plain)
        zfree(val);
    else
        sdsfree(val);
    return NULL;
}

/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
ssize_t rdbSaveRawString(rio *rdb, const unsigned char *s, size_t len) {
    int enclen;
    ssize_t n, nwritten = 0;

    /* Try integer encoding */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            if (rdbWriteRaw(rdb,buf,enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (g_pserver->rdb_compression && len > 20) {
        n = rdbSaveLzfStringObject(rdb,(const unsigned char*)s,len);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    if ((n = rdbSaveLen(rdb,len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (rdbWriteRaw(rdb,(unsigned char*)s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* Save a long long value as either an encoded string or a string. */
ssize_t rdbSaveLongLongAsStringObject(rio *rdb, long long value) {
    unsigned char buf[32];
    ssize_t n, nwritten = 0;
    int enclen = rdbEncodeInteger(value,buf);
    if (enclen > 0) {
        return rdbWriteRaw(rdb,buf,enclen);
    } else {
        /* Encode as string */
        enclen = ll2string((char*)buf,32,value);
        serverAssert(enclen < 32);
        if ((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n;
        if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

/* Like rdbSaveRawString() gets a Redis object instead. */
ssize_t rdbSaveStringObject(rio *rdb, robj_roptr obj) {
    /* Avoid to decode the object, then encode it again, if the
     * object is already integer encoded. */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(rdb,(long)ptrFromObj(obj));
    } else {
        serverAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
        return rdbSaveRawString(rdb,(unsigned char*)szFromObj(obj),sdslen(szFromObj(obj)));
    }
}

/* Load a string object from an RDB file according to flags:
 *
 * RDB_LOAD_NONE (no flags): load an RDB object, unencoded.
 * RDB_LOAD_ENC: If the returned type is a Redis object, try to
 *               encode it in a special way to be more memory
 *               efficient. When this flag is passed the function
 *               no longer guarantees that ptrFromObj(obj) is an SDS string.
 * RDB_LOAD_PLAIN: Return a plain string allocated with zmalloc()
 *                 instead of a Redis object with an sds in it.
 * RDB_LOAD_SDS: Return an SDS string instead of a Redis object.
 *
 * On I/O error NULL is returned.
 */
void *rdbGenericLoadStringObject(rio *rdb, int flags, size_t *lenptr) {
    int encode = flags & RDB_LOAD_ENC;
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int isencoded;
    unsigned long long len;

    len = rdbLoadLen(rdb,&isencoded);
    if (len == RDB_LENERR) return NULL;

    if (isencoded) {
        switch(len) {
        case RDB_ENC_INT8:
        case RDB_ENC_INT16:
        case RDB_ENC_INT32:
            return rdbLoadIntegerObject(rdb,len,flags,lenptr);
        case RDB_ENC_LZF:
            return rdbLoadLzfStringObject(rdb,flags,lenptr);
        default:
            rdbReportCorruptRDB("Unknown RDB string encoding type %llu",len);
            return NULL;
        }
    }

    if (plain || sds) {
        void *buf = plain ? ztrymalloc(len) : sdstrynewlen(SDS_NOINIT,len);
        if (!buf) {
            serverLog(g_pserver->loading? LL_WARNING: LL_VERBOSE, "rdbGenericLoadStringObject failed allocating %llu bytes", len);
            return NULL;
        }
        if (lenptr) *lenptr = len;
        if (len && rioRead(rdb,buf,len) == 0) {
            if (plain)
                zfree(buf);
            else
                sdsfree((char*)buf);
            return NULL;
        }
        return buf;
    } else {
        robj *o = encode ? createStringObject(SDS_NOINIT,len) :
                           createRawStringObject(SDS_NOINIT,len);
        if (len && rioRead(rdb,ptrFromObj(o),len) == 0) {
            decrRefCount(o);
            return NULL;
        }
        return o;
    }
}

sdsstring rdbLoadString(rio *rdb){
    sds str = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
    return sdsstring(str);
}

robj *rdbLoadStringObject(rio *rdb) {
    return (robj*)rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE,NULL);
}

robj *rdbLoadEncodedStringObject(rio *rdb) {
    return (robj*)rdbGenericLoadStringObject(rdb,RDB_LOAD_ENC,NULL);
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
int rdbSaveDoubleValue(rio *rdb, double val) {
    unsigned char buf[128];
    int len;

    if (std::isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!std::isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(rdb,buf,len);
}

/* For information about double serialization check rdbSaveDoubleValue() */
int rdbLoadDoubleValue(rio *rdb, double *val) {
    char buf[256];
    unsigned char len;

    if (rioRead(rdb,&len,1) == 0) return -1;
    switch(len) {
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    default:
        if (rioRead(rdb,buf,len) == 0) return -1;
        buf[len] = '\0';
        if (sscanf(buf, "%lg", val)!=1) return -1;
        return 0;
    }
}

/* Saves a double for RDB 8 or greater, where IE754 binary64 format is assumed.
 * We just make sure the integer is always stored in little endian, otherwise
 * the value is copied verbatim from memory to disk.
 *
 * Return -1 on error, the size of the serialized value on success. */
int rdbSaveBinaryDoubleValue(rio *rdb, double val) {
    memrev64ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
}

/* Loads a double from RDB 8 or greater. See rdbSaveBinaryDoubleValue() for
 * more info. On error -1 is returned, otherwise 0. */
int rdbLoadBinaryDoubleValue(rio *rdb, double *val) {
    if (rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev64ifbe(val);
    return 0;
}

/* Like rdbSaveBinaryDoubleValue() but single precision. */
int rdbSaveBinaryFloatValue(rio *rdb, float val) {
    memrev32ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
}

/* Like rdbLoadBinaryDoubleValue() but single precision. */
int rdbLoadBinaryFloatValue(rio *rdb, float *val) {
    if (rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev32ifbe(val);
    return 0;
}

/* Save the object type of object "o". */
int rdbSaveObjectType(rio *rdb, robj_roptr o) {
    switch (o->type) {
    case OBJ_STRING:
        return rdbSaveType(rdb,RDB_TYPE_STRING);
    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST)
            return rdbSaveType(rdb,RDB_TYPE_LIST_QUICKLIST);
        else
            serverPanic("Unknown list encoding");
    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET)
            return rdbSaveType(rdb,RDB_TYPE_SET_INTSET);
        else if (o->encoding == OBJ_ENCODING_HT)
            return rdbSaveType(rdb,RDB_TYPE_SET);
        else
            serverPanic("Unknown set encoding");
    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST)
            return rdbSaveType(rdb,RDB_TYPE_ZSET_ZIPLIST);
        else if (o->encoding == OBJ_ENCODING_SKIPLIST)
            return rdbSaveType(rdb,RDB_TYPE_ZSET_2);
        else
            serverPanic("Unknown sorted set encoding");
    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_ZIPLIST)
            return rdbSaveType(rdb,RDB_TYPE_HASH_ZIPLIST);
        else if (o->encoding == OBJ_ENCODING_HT)
            return rdbSaveType(rdb,RDB_TYPE_HASH);
        else
            serverPanic("Unknown hash encoding");
    case OBJ_STREAM:
        return rdbSaveType(rdb,RDB_TYPE_STREAM_LISTPACKS);
    case OBJ_MODULE:
        return rdbSaveType(rdb,RDB_TYPE_MODULE_2);
    case OBJ_CRON:
        return rdbSaveType(rdb,RDB_TYPE_CRON);
    default:
        serverPanic("Unknown object type");
    }
    return -1; /* avoid warning */
}

/* Use rdbLoadType() to load a TYPE in RDB format, but returns -1 if the
 * type is not specifically a valid Object Type. */
int rdbLoadObjectType(rio *rdb) {
    int type;
    if ((type = rdbLoadType(rdb)) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}

/* This helper function serializes a consumer group Pending Entries List (PEL)
 * into the RDB file. The 'nacks' argument tells the function if also persist
 * the informations about the not acknowledged message, or if to persist
 * just the IDs: this is useful because for the global consumer group PEL
 * we serialized the NACKs as well, but when serializing the local consumer
 * PELs we just add the ID, that will be resolved inside the global PEL to
 * put a reference to the same structure. */
ssize_t rdbSaveStreamPEL(rio *rdb, rax *pel, int nacks) {
    ssize_t n, nwritten = 0;

    /* Number of entries in the PEL. */
    if ((n = rdbSaveLen(rdb,raxSize(pel))) == -1) return -1;
    nwritten += n;

    /* Save each entry. */
    raxIterator ri;
    raxStart(&ri,pel);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        /* We store IDs in raw form as 128 big big endian numbers, like
         * they are inside the radix tree key. */
        if ((n = rdbWriteRaw(rdb,ri.key,sizeof(streamID))) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;

        if (nacks) {
            streamNACK *nack = (streamNACK*)ri.data;
            if ((n = rdbSaveMillisecondTime(rdb,nack->delivery_time)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
            if ((n = rdbSaveLen(rdb,nack->delivery_count)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
            /* We don't save the consumer name: we'll save the pending IDs
             * for each consumer in the consumer PEL, and resolve the consumer
             * at loading time. */
        }
    }
    raxStop(&ri);
    return nwritten;
}

/* Serialize the consumers of a stream consumer group into the RDB. Helper
 * function for the stream data type serialization. What we do here is to
 * persist the consumer metadata, and it's PEL, for each consumer. */
size_t rdbSaveStreamConsumers(rio *rdb, streamCG *cg) {
    ssize_t n, nwritten = 0;

    /* Number of consumers in this consumer group. */
    if ((n = rdbSaveLen(rdb,raxSize(cg->consumers))) == -1) return -1;
    nwritten += n;

    /* Save each consumer. */
    raxIterator ri;
    raxStart(&ri,cg->consumers);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        streamConsumer *consumer = (streamConsumer*)ri.data;

        /* Consumer name. */
        if ((n = rdbSaveRawString(rdb,ri.key,ri.key_len)) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;

        /* Last seen time. */
        if ((n = rdbSaveMillisecondTime(rdb,consumer->seen_time)) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;

        /* Consumer PEL, without the ACKs (see last parameter of the function
         * passed with value of 0), at loading time we'll lookup the ID
         * in the consumer group global PEL and will put a reference in the
         * consumer local PEL. */
        if ((n = rdbSaveStreamPEL(rdb,consumer->pel,0)) == -1) {
            raxStop(&ri);
            return -1;
        }
        nwritten += n;
    }
    raxStop(&ri);
    return nwritten;
}

/* Save a Redis object.
 * Returns -1 on error, number of bytes written on success. */
ssize_t rdbSaveObject(rio *rdb, robj_roptr o, robj *key) {
    ssize_t n = 0, nwritten = 0;

    if (o->type == OBJ_STRING) {
        /* Save a string value */
        if ((n = rdbSaveStringObject(rdb,o)) == -1) return -1;
        nwritten += n;
    } else if (o->type == OBJ_LIST) {
        /* Save a list value */
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = (quicklist*)ptrFromObj(o);
            quicklistNode *node = ql->head;

            if ((n = rdbSaveLen(rdb,ql->len)) == -1) return -1;
            nwritten += n;

            while(node) {
                if (quicklistNodeIsCompressed(node)) {
                    void *data;
                    size_t compress_len = quicklistGetLzf(node, &data);
                    if ((n = rdbSaveLzfBlob(rdb,data,compress_len,node->sz)) == -1) return -1;
                    nwritten += n;
                } else {
                    if ((n = rdbSaveRawString(rdb,node->zl,node->sz)) == -1) return -1;
                    nwritten += n;
                }
                node = node->next;
            }
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        /* Save a set value */
        if (o->encoding == OBJ_ENCODING_HT) {
            dict *set = (dict*)ptrFromObj(o);
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize(set))) == -1) {
                dictReleaseIterator(di);
                return -1;
            }
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                sds ele = (sds)dictGetKey(de);
                if ((n = rdbSaveRawString(rdb,(unsigned char*)ele,sdslen(ele)))
                    == -1)
                {
                    dictReleaseIterator(di);
                    return -1;
                }
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)ptrFromObj(o));

            if ((n = rdbSaveRawString(rdb,(unsigned char*)szFromObj(o),l)) == -1) return -1;
            nwritten += n;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        /* Save a sorted set value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)ptrFromObj(o));

            if ((n = rdbSaveRawString(rdb,(unsigned char*)ptrFromObj(o),l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = (zset*)ptrFromObj(o);
            zskiplist *zsl = zs->zsl;

            if ((n = rdbSaveLen(rdb,zsl->length)) == -1) return -1;
            nwritten += n;

            /* We save the skiplist elements from the greatest to the smallest
             * (that's trivial since the elements are already ordered in the
             * skiplist): this improves the load process, since the next loaded
             * element will always be the smaller, so adding to the skiplist
             * will always immediately stop at the head, making the insertion
             * O(1) instead of O(log(N)). */
            zskiplistNode *zn = zsl->tail;
            while (zn != NULL) {
                if ((n = rdbSaveRawString(rdb,
                    (unsigned char*)zn->ele,sdslen(zn->ele))) == -1)
                {
                    return -1;
                }
                nwritten += n;
                if ((n = rdbSaveBinaryDoubleValue(rdb,zn->score)) == -1)
                    return -1;
                nwritten += n;
                zn = zn->backward;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        /* Save a hash value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)ptrFromObj(o));

            if ((n = rdbSaveRawString(rdb,(unsigned char*)ptrFromObj(o),l)) == -1) return -1;
            nwritten += n;

        } else if (o->encoding == OBJ_ENCODING_HT) {
            dictIterator *di = dictGetIterator((dict*)ptrFromObj(o));
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize((dict*)ptrFromObj(o)))) == -1) {
                dictReleaseIterator(di);
                return -1;
            }
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                sds field = (sds)dictGetKey(de);
                sds value = (sds)dictGetVal(de);

                if ((n = rdbSaveRawString(rdb,(unsigned char*)field,
                        sdslen(field))) == -1)
                {
                    dictReleaseIterator(di);
                    return -1;
                }
                nwritten += n;
                if ((n = rdbSaveRawString(rdb,(unsigned char*)value,
                        sdslen(value))) == -1)
                {
                    dictReleaseIterator(di);
                    return -1;
                }
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (o->type == OBJ_STREAM) {
        /* Store how many listpacks we have inside the radix tree. */
        stream *s = (stream*)ptrFromObj(o);
        rax *rax = s->rax;
        if ((n = rdbSaveLen(rdb,raxSize(rax))) == -1) return -1;
        nwritten += n;

        /* Serialize all the listpacks inside the radix tree as they are,
         * when loading back, we'll use the first entry of each listpack
         * to insert it back into the radix tree. */
        raxIterator ri;
        raxStart(&ri,rax);
        raxSeek(&ri,"^",NULL,0);
        while (raxNext(&ri)) {
            unsigned char *lp = (unsigned char*)ri.data;
            size_t lp_bytes = lpBytes(lp);
            if ((n = rdbSaveRawString(rdb,ri.key,ri.key_len)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
            if ((n = rdbSaveRawString(rdb,lp,lp_bytes)) == -1) {
                raxStop(&ri);
                return -1;
            }
            nwritten += n;
        }
        raxStop(&ri);

        /* Save the number of elements inside the stream. We cannot obtain
         * this easily later, since our macro nodes should be checked for
         * number of items: not a great CPU / space tradeoff. */
        if ((n = rdbSaveLen(rdb,s->length)) == -1) return -1;
        nwritten += n;
        /* Save the last entry ID. */
        if ((n = rdbSaveLen(rdb,s->last_id.ms)) == -1) return -1;
        nwritten += n;
        if ((n = rdbSaveLen(rdb,s->last_id.seq)) == -1) return -1;
        nwritten += n;

        /* The consumer groups and their clients are part of the stream
         * type, so serialize every consumer group. */

        /* Save the number of groups. */
        size_t num_cgroups = s->cgroups ? raxSize(s->cgroups) : 0;
        if ((n = rdbSaveLen(rdb,num_cgroups)) == -1) return -1;
        nwritten += n;

        if (num_cgroups) {
            /* Serialize each consumer group. */
            raxStart(&ri,s->cgroups);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                streamCG *cg = (streamCG*)ri.data;

                /* Save the group name. */
                if ((n = rdbSaveRawString(rdb,ri.key,ri.key_len)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;

                /* Last ID. */
                if ((n = rdbSaveLen(rdb,cg->last_id.ms)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;
                if ((n = rdbSaveLen(rdb,cg->last_id.seq)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;

                /* Save the global PEL. */
                if ((n = rdbSaveStreamPEL(rdb,cg->pel,1)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;

                /* Save the consumers of this group. */
                if ((n = rdbSaveStreamConsumers(rdb,cg)) == -1) {
                    raxStop(&ri);
                    return -1;
                }
                nwritten += n;
            }
            raxStop(&ri);
        }
    } else if (o->type == OBJ_MODULE) {
        /* Save a module-specific value. */
        RedisModuleIO io;
        moduleValue *mv = (moduleValue*)ptrFromObj(o);
        moduleType *mt = mv->type;

        /* Write the "module" identifier as prefix, so that we'll be able
         * to call the right module during loading. */
        int retval = rdbSaveLen(rdb,mt->id);
        if (retval == -1) return -1;
        moduleInitIOContext(io,mt,rdb,key);
        io.bytes += retval;

        /* Then write the module-specific representation + EOF marker. */
        mt->rdb_save(&io,mv->value);
        retval = rdbSaveLen(rdb,RDB_MODULE_OPCODE_EOF);
        if (retval == -1)
            io.error = 1;
        else
            io.bytes += retval;

        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }
        return io.error ? -1 : (ssize_t)io.bytes;
    } else if (o->type == OBJ_CRON) {
        cronjob *job = (cronjob*)ptrFromObj(o);
        rdbSaveRawString(rdb, (const unsigned char*)job->script.get(), job->script.size());
        rdbSaveMillisecondTime(rdb, job->startTime);
        rdbSaveMillisecondTime(rdb, job->interval);
        rdbSaveLen(rdb, job->veckeys.size());
        for (auto &key : job->veckeys)
            rdbSaveRawString(rdb, (const unsigned char*)key.get(), key.size());
        rdbSaveLen(rdb, job->vecargs.size());
        for (auto &arg : job->vecargs)
            rdbSaveRawString(rdb, (const unsigned char*)arg.get(), arg.size());
    } else {
        serverPanic("Unknown object type");
    }
    return nwritten;
}

/* Save an AUX field. */
ssize_t rdbSaveAuxField(rio *rdb, const void *key, size_t keylen, const void *val, size_t vallen) {
    ssize_t ret, len = 0;
    if ((ret = rdbSaveType(rdb,RDB_OPCODE_AUX)) == -1) return -1;
    len += ret;
    if ((ret = rdbSaveRawString(rdb,(const unsigned char*)key,keylen)) == -1) return -1;
    len += ret;
    if ((ret = rdbSaveRawString(rdb,(const unsigned char*)val,vallen)) == -1) return -1;
    len += ret;
    return len;
}

/* Wrapper for rdbSaveAuxField() used when key/val length can be obtained
 * with strlen(). */
ssize_t rdbSaveAuxFieldStrStr(rio *rdb, const char *key, const char *val) {
    return rdbSaveAuxField(rdb,key,strlen(key),val,strlen(val));
}

/* Wrapper for strlen(key) + integer type (up to long long range). */
ssize_t rdbSaveAuxFieldStrInt(rio *rdb, const char *key, long long val) {
    char buf[LONG_STR_SIZE];
    int vlen = ll2string(buf,sizeof(buf),val);
    return rdbSaveAuxField(rdb,key,strlen(key),buf,vlen);
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. */
size_t rdbSavedObjectLen(robj *o, robj *key) {
    ssize_t len = rdbSaveObject(NULL,o,key);
    serverAssertWithInfo(NULL,o,len != -1);
    return len;
}

/* Save a key-value pair, with expire time, type, key, value.
 * On error -1 is returned.
 * On success if the key was actually saved 1 is returned. */
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val, expireEntry *pexpire) {
    int savelru = g_pserver->maxmemory_policy & MAXMEMORY_FLAG_LRU;
    int savelfu = g_pserver->maxmemory_policy & MAXMEMORY_FLAG_LFU;

    /* Save the expire time */
    long long expiretime = INVALID_EXPIRE;
    if (pexpire != nullptr && pexpire->FGetPrimaryExpire(&expiretime)) {
        if (rdbSaveType(rdb,RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if (rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
    }

    /* Save the LRU info. */
    if (savelru) {
        uint64_t idletime = estimateObjectIdleTime(val);
        idletime /= 1000; /* Using seconds is enough and requires less space.*/
        if (rdbSaveType(rdb,RDB_OPCODE_IDLE) == -1) return -1;
        if (rdbSaveLen(rdb,idletime) == -1) return -1;
    }

    /* Save the LFU info. */
    if (savelfu) {
        uint8_t buf[1];
        buf[0] = LFUDecrAndReturn(val);
        /* We can encode this in exactly two bytes: the opcode and an 8
         * bit counter, since the frequency is logarithmic with a 0-255 range.
         * Note that we do not store the halving time because to reset it
         * a single time when loading does not affect the frequency much. */
        if (rdbSaveType(rdb,RDB_OPCODE_FREQ) == -1) return -1;
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
    }

    char szT[32];
    if (g_pserver->fActiveReplica) {
        snprintf(szT, 32, "%" PRIu64, mvccFromObj(val));
        if (rdbSaveAuxFieldStrStr(rdb,"mvcc-tstamp", szT) == -1) return -1;
    }

    /* Save type, key, value */
    if (rdbSaveObjectType(rdb,val) == -1) return -1;
    if (rdbSaveStringObject(rdb,key) == -1) return -1;
    if (rdbSaveObject(rdb,val,key) == -1) return -1;

    /* Delay return if required (for testing) */
    if (g_pserver->rdb_key_save_delay)
        debugDelay(g_pserver->rdb_key_save_delay);

    /* Save expire entry after as it will apply to the previously loaded key */
    /*  This is because we update the expire datastructure directly without buffering */
    if (pexpire != nullptr)
    {
        for (auto itr : *pexpire)
        {
            if (itr.subkey() == nullptr)
                continue;   // already saved
            snprintf(szT, 32, "%lld", itr.when());
            rdbSaveAuxFieldStrStr(rdb,"keydb-subexpire-key",itr.subkey());
            rdbSaveAuxFieldStrStr(rdb,"keydb-subexpire-when",szT);
        }
    }

    return 1;
}

/* Save a few default AUX fields with information about the RDB generated. */
int rdbSaveInfoAuxFields(rio *rdb, int rdbflags, rdbSaveInfo *rsi) {
    int redis_bits = (sizeof(void*) == 8) ? 64 : 32;
    int aof_preamble = (rdbflags & RDBFLAGS_AOF_PREAMBLE) != 0;

    /* Add a few fields about the state when the RDB was created. */
    if (rdbSaveAuxFieldStrStr(rdb,"redis-ver",KEYDB_REAL_VERSION) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"redis-bits",redis_bits) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"ctime",time(NULL)) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"used-mem",zmalloc_used_memory()) == -1) return -1;

    /* Handle saving options that generate aux fields. */
    if (rsi) {
        if (rdbSaveAuxFieldStrInt(rdb,"repl-stream-db",rsi->repl_stream_db)
            == -1) return -1;
        if (rdbSaveAuxFieldStrStr(rdb,"repl-id",g_pserver->replid)
            == -1) return -1;
        if (rdbSaveAuxFieldStrInt(rdb,"repl-offset",g_pserver->master_repl_offset)
            == -1) return -1;
    }
    if (rdbSaveAuxFieldStrInt(rdb,"aof-preamble",aof_preamble) == -1) return -1;
    return 1;
}

int saveKey(rio *rdb, redisDb *db, int flags, size_t *processed, const char *keystr, robj *o)
{    
    redisObjectStack key;

    initStaticStringObject(key,(char*)keystr);
    expireEntry *pexpire = getExpire(db, &key);

    if (rdbSaveKeyValuePair(rdb,&key,o,pexpire) == -1)
        return 0;

    /* When this RDB is produced as part of an AOF rewrite, move
        * accumulated diff from parent to child while rewriting in
        * order to have a smaller final write. */
    if (flags & RDBFLAGS_AOF_PREAMBLE &&
        rdb->processed_bytes > *processed+AOF_READ_DIFF_INTERVAL_BYTES)
    {
        *processed = rdb->processed_bytes;
        aofReadDiffFromParent();
    }
    return 1;
}

ssize_t rdbSaveSingleModuleAux(rio *rdb, int when, moduleType *mt) {
    /* Save a module-specific aux value. */
    RedisModuleIO io;
    int retval = rdbSaveType(rdb, RDB_OPCODE_MODULE_AUX);
    if (retval == -1) return -1;
    moduleInitIOContext(io,mt,rdb,NULL);
    io.bytes += retval;

    /* Write the "module" identifier as prefix, so that we'll be able
     * to call the right module during loading. */
    retval = rdbSaveLen(rdb,mt->id);
    if (retval == -1) return -1;
    io.bytes += retval;

    /* write the 'when' so that we can provide it on loading. add a UINT opcode
     * for backwards compatibility, everything after the MT needs to be prefixed
     * by an opcode. */
    retval = rdbSaveLen(rdb,RDB_MODULE_OPCODE_UINT);
    if (retval == -1) return -1;
    io.bytes += retval;
    retval = rdbSaveLen(rdb,when);
    if (retval == -1) return -1;
    io.bytes += retval;

    /* Then write the module-specific representation + EOF marker. */
    mt->aux_save(&io,when);
    retval = rdbSaveLen(rdb,RDB_MODULE_OPCODE_EOF);
    if (retval == -1)
        io.error = 1;
    else
        io.bytes += retval;

    if (io.ctx) {
        moduleFreeContext(io.ctx);
        zfree(io.ctx);
    }
    if (io.error)
        return -1;
    return io.bytes;
}

/* Produces a dump of the database in RDB format sending it to the specified
 * Redis I/O channel. On success C_OK is returned, otherwise C_ERR
 * is returned and part of the output, or all the output, can be
 * missing because of I/O errors.
 *
 * When the function returns C_ERR and if 'error' is not NULL, the
 * integer pointed by 'error' is set to the value of errno just after the I/O
 * error. */
int rdbSaveRio(rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi) {
    dictIterator *di = NULL;
    dictEntry *de;
    char magic[10];
    uint64_t cksum;
    size_t processed = 0;
    int j;
    long key_count = 0;
    long long info_updated_time = 0;
    const char *pname = (rdbflags & RDBFLAGS_AOF_PREAMBLE) ? "AOF rewrite" :  "RDB";

    if (g_pserver->rdb_checksum)
        rdb->update_cksum = rioGenericUpdateChecksum;
    snprintf(magic,sizeof(magic),"REDIS%04d",RDB_VERSION);
    if (rdbWriteRaw(rdb,magic,9) == -1) goto werr;
    if (rdbSaveInfoAuxFields(rdb,rdbflags,rsi) == -1) goto werr;
    if (rdbSaveModulesAux(rdb, REDISMODULE_AUX_BEFORE_RDB) == -1) goto werr;

    for (j = 0; j < cserver.dbnum; j++) {
        redisDb *db = g_pserver->db+j;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);

        /* Write the SELECT DB opcode */
        if (rdbSaveType(rdb,RDB_OPCODE_SELECTDB) == -1) goto werr;
        if (rdbSaveLen(rdb,j) == -1) goto werr;

        /* Write the RESIZE DB opcode. */
        uint64_t db_size, expires_size;
        db_size = dictSize(db->dict);
        expires_size = db->setexpire->size();
        if (rdbSaveType(rdb,RDB_OPCODE_RESIZEDB) == -1) goto werr;
        if (rdbSaveLen(rdb,db_size) == -1) goto werr;
        if (rdbSaveLen(rdb,expires_size) == -1) goto werr;
        
        /* Iterate this DB writing every entry */
        size_t ckeysExpired = 0;
        while((de = dictNext(di)) != NULL) {
            sds keystr = (sds)dictGetKey(de);
            robj *o = (robj*)dictGetVal(de);

            if (o->FExpires())
                ++ckeysExpired;
            
            if (!saveKey(rdb, db, rdbflags, &processed, keystr, o))
                goto werr;

            /* Update child info every 1 second (approximately).
             * in order to avoid calling mstime() on each iteration, we will
             * check the diff every 1024 keys */
            if ((key_count++ & 1023) == 0) {
                long long now = mstime();
                if (now - info_updated_time >= 1000) {
                    sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, pname);
                    info_updated_time = now;
                }
            }
        }
        serverAssert(ckeysExpired == db->setexpire->size());
        dictReleaseIterator(di);
        di = NULL; /* So that we don't release it again on error. */
    }

    /* If we are storing the replication information on disk, persist
     * the script cache as well: on successful PSYNC after a restart, we need
     * to be able to process any EVALSHA inside the replication backlog the
     * master will send us. */
    if (rsi && dictSize(g_pserver->lua_scripts)) {
        di = dictGetIterator(g_pserver->lua_scripts);
        while((de = dictNext(di)) != NULL) {
            robj *body = (robj*)dictGetVal(de);
            if (rdbSaveAuxField(rdb,"lua",3,szFromObj(body),sdslen(szFromObj(body))) == -1)
                goto werr;
        }
        dictReleaseIterator(di);
        di = NULL; /* So that we don't release it again on error. */
    }

    if (rdbSaveModulesAux(rdb, REDISMODULE_AUX_AFTER_RDB) == -1) goto werr;

    /* EOF opcode */
    if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. */
    cksum = rdb->cksum;
    memrev64ifbe(&cksum);
    if (rioWrite(rdb,&cksum,8) == 0) goto werr;
    return C_OK;

werr:
    if (error) *error = errno;
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

/* This is just a wrapper to rdbSaveRio() that additionally adds a prefix
 * and a suffix to the generated RDB dump. The prefix is:
 *
 * $EOF:<40 bytes unguessable hex string>\r\n
 *
 * While the suffix is the 40 bytes hex string we announced in the prefix.
 * This way processes receiving the payload can understand when it ends
 * without doing any processing of the content. */
int rdbSaveRioWithEOFMark(rio *rdb, int *error, rdbSaveInfo *rsi) {
    char eofmark[RDB_EOF_MARK_SIZE];

    startSaving(RDBFLAGS_REPLICATION);
    getRandomHexChars(eofmark,RDB_EOF_MARK_SIZE);
    if (error) *error = 0;
    if (rioWrite(rdb,"$EOF:",5) == 0) goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    if (rioWrite(rdb,"\r\n",2) == 0) goto werr;
    if (rdbSaveRio(rdb,error,RDBFLAGS_NONE,rsi) == C_ERR) goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    stopSaving(1);
    return C_OK;

werr: /* Write error. */
    /* Set 'error' only if not already set by rdbSaveRio() call. */
    if (error && *error == 0) *error = errno;
    stopSaving(0);
    return C_ERR;
}

int rdbSaveFp(FILE *fp, rdbSaveInfo *rsi)
{
    int error = 0;
    rio rdb;

    rioInitWithFile(&rdb,fp);

    if (g_pserver->rdb_save_incremental_fsync)
        rioSetAutoSync(&rdb,REDIS_AUTOSYNC_BYTES);

    if (rdbSaveRio(&rdb,&error,RDBFLAGS_NONE,rsi) == C_ERR) {
        errno = error;
        return C_ERR;
    }
    return C_OK;
}

int rdbSave(rdbSaveInfo *rsi)
{
    int err = C_OK;
    if (g_pserver->rdb_filename != NULL)
        err = rdbSaveFile(g_pserver->rdb_filename, rsi);

    if (err == C_OK && g_pserver->rdb_s3bucketpath != NULL)
        err = rdbSaveS3(g_pserver->rdb_s3bucketpath, rsi);
    return err;
}

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
int rdbSaveFile(char *filename, rdbSaveInfo *rsi) {
    char tmpfile[256];
    char cwd[MAXPATHLEN]; /* Current working dir path for error messages. */
    FILE *fp = NULL;
    rio rdb;
    int error = 0;

    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        char *cwdp = getcwd(cwd,MAXPATHLEN);
        serverLog(LL_WARNING,
            "Failed opening the RDB file %s (in server root dir %s) "
            "for saving: %s",
            filename,
            cwdp ? cwdp : "unknown",
            strerror(errno));
        return C_ERR;
    }

    rioInitWithFile(&rdb,fp);
    startSaving(RDBFLAGS_NONE);

    if (g_pserver->rdb_save_incremental_fsync)
        rioSetAutoSync(&rdb,REDIS_AUTOSYNC_BYTES);

    if (rdbSaveRio(&rdb,&error,RDBFLAGS_NONE,rsi) == C_ERR) {
        errno = error;
        goto werr;
    }

    /* Make sure data will not remain on the OS's output buffers */
    if (fflush(fp)) goto werr;
    if (fsync(fileno(fp))) goto werr;
    if (fclose(fp)) { fp = NULL; goto werr; }
    fp = NULL;
    
    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    if (rename(tmpfile,filename) == -1) {
        char *cwdp = getcwd(cwd,MAXPATHLEN);
        serverLog(LL_WARNING,
            "Error moving temp DB file %s on the final "
            "destination %s (in server root dir %s): %s",
            tmpfile,
            filename,
            cwdp ? cwdp : "unknown",
            strerror(errno));
        unlink(tmpfile);
        stopSaving(0);
        return C_ERR;
    }

    serverLog(LL_NOTICE,"DB saved on disk");
    g_pserver->dirty = 0;
    g_pserver->lastsave = time(NULL);
    g_pserver->lastbgsave_status = C_OK;
    stopSaving(1);
    return C_OK;

werr:
    serverLog(LL_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    if (fp) fclose(fp);
    unlink(tmpfile);
    stopSaving(0);
    return C_ERR;
}

int rdbSaveBackground(rdbSaveInfo *rsi) {
    pid_t childpid;

    if (hasActiveChildProcess()) return C_ERR;

    g_pserver->dirty_before_bgsave = g_pserver->dirty;
    g_pserver->lastbgsave_try = time(NULL);

    if ((childpid = redisFork(CHILD_TYPE_RDB)) == 0) {
        int retval;

        /* Child */
        redisSetProcTitle("keydb-rdb-bgsave");
        redisSetCpuAffinity(g_pserver->bgsave_cpulist);
        retval = rdbSave(rsi);
        if (retval == C_OK) {
            sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
        }
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        if (childpid == -1) {
            g_pserver->lastbgsave_status = C_ERR;
            serverLog(LL_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE,"Background saving started by pid %d",childpid);
        g_pserver->rdb_save_time_start = time(NULL);
        g_pserver->rdb_child_type = RDB_CHILD_TYPE_DISK;
        updateDictResizePolicy();
        return C_OK;
    }
    return C_OK; /* unreached */
}

/* Note that we may call this function in signal handle 'sigShutdownHandler',
 * so we need guarantee all functions we call are async-signal-safe.
 * If  we call this function from signal handle, we won't call bg_unlink that
 * is not async-signal-safe. */
void rdbRemoveTempFile(pid_t childpid, int from_signal) {
    char tmpfile[256];
    char pid[32];

    /* Generate temp rdb file name using aync-signal safe functions. */
    int pid_len = ll2string(pid, sizeof(pid), childpid);
    strcpy(tmpfile, "temp-");
    strncpy(tmpfile+5, pid, pid_len);
    strcpy(tmpfile+5+pid_len, ".rdb");

    if (from_signal) {
        /* bg_unlink is not async-signal-safe, but in this case we don't really
         * need to close the fd, it'll be released when the process exists. */
        int fd = open(tmpfile, O_RDONLY|O_NONBLOCK);
        UNUSED(fd);
        unlink(tmpfile);
    } else {
        bg_unlink(tmpfile);
    }
}

/* This function is called by rdbLoadObject() when the code is in RDB-check
 * mode and we find a module value of type 2 that can be parsed without
 * the need of the actual module. The value is parsed for errors, finally
 * a dummy redis object is returned just to conform to the API. */
robj *rdbLoadCheckModuleValue(rio *rdb, char *modulename) {
    uint64_t opcode;
    while((opcode = rdbLoadLen(rdb,NULL)) != RDB_MODULE_OPCODE_EOF) {
        if (opcode == RDB_MODULE_OPCODE_SINT ||
            opcode == RDB_MODULE_OPCODE_UINT)
        {
            uint64_t len;
            if (rdbLoadLenByRef(rdb,NULL,&len) == -1) {
                rdbReportCorruptRDB(
                    "Error reading integer from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_STRING) {
            robj *o = (robj*)rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE,NULL);
            if (o == NULL) {
                rdbReportCorruptRDB(
                    "Error reading string from module %s value", modulename);
            }
            decrRefCount(o);
        } else if (opcode == RDB_MODULE_OPCODE_FLOAT) {
            float val;
            if (rdbLoadBinaryFloatValue(rdb,&val) == -1) {
                rdbReportCorruptRDB(
                    "Error reading float from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_DOUBLE) {
            double val;
            if (rdbLoadBinaryDoubleValue(rdb,&val) == -1) {
                rdbReportCorruptRDB(
                    "Error reading double from module %s value", modulename);
            }
        }
    }
    return createStringObject("module-dummy-value",18);
}

/* Load a Redis object of the specified type from the specified file.
 * On success a newly allocated object is returned, otherwise NULL. */
robj *rdbLoadObject(int rdbtype, rio *rdb, sds key, uint64_t mvcc_tstamp) {
    robj *o = NULL, *ele, *dec;
    uint64_t len;
    unsigned int i;

    int deep_integrity_validation = cserver.sanitize_dump_payload == SANITIZE_DUMP_YES;
    if (cserver.sanitize_dump_payload == SANITIZE_DUMP_CLIENTS) {
        /* Skip sanitization when loading (an RDB), or getting a RESTORE command
         * from either the master or a client using an ACL user with the skip-sanitize-payload flag. */
        int skip = g_pserver->loading ||
            (serverTL->current_client && (serverTL->current_client->flags & CLIENT_MASTER));
        if (!skip && serverTL->current_client && serverTL->current_client->user)
            skip = !!(serverTL->current_client->user->flags & USER_FLAG_SANITIZE_PAYLOAD_SKIP);
        deep_integrity_validation = !skip;
    }

    if (rdbtype == RDB_TYPE_STRING) {
        /* Read string value */
        if ((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncoding(o);
    } else if (rdbtype == RDB_TYPE_LIST) {
        /* Read list value */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;

        o = createQuicklistObject();
        quicklistSetOptions((quicklist*)ptrFromObj(o), g_pserver->list_max_ziplist_size,
                            g_pserver->list_compress_depth);

        /* Load every single element of the list */
        while(len--) {
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) {
                decrRefCount(o);
                return NULL;
            }
            dec = getDecodedObject(ele);
            size_t len = sdslen(szFromObj(dec));
            quicklistPushTail((quicklist*)ptrFromObj(o), ptrFromObj(dec), len);
            decrRefCount(dec);
            decrRefCount(ele);
        }
    } else if (rdbtype == RDB_TYPE_SET) {
        /* Read Set value */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;

        /* Use a regular set when there are too many entries. */
        if (len > g_pserver->set_max_intset_entries) {
            o = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            if (len > DICT_HT_INITIAL_SIZE && dictTryExpand((dict*)ptrFromObj(o),len) != DICT_OK) {
                rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)len);
                decrRefCount(o);
                return NULL;
            }
        } else {
            o = createIntsetObject();
        }

        /* Load every single element of the set */
        for (i = 0; i < len; i++) {
            long long llval;
            sds sdsele;

            if ((sdsele = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                decrRefCount(o);
                return NULL;
            }

            if (o->encoding == OBJ_ENCODING_INTSET) {
                /* Fetch integer value from element. */
                if (isSdsRepresentableAsLongLong(sdsele,&llval) == C_OK) {
                    uint8_t success;
                    o->m_ptr = intsetAdd((intset*)ptrFromObj(o),llval,&success);
                    if (!success) {
                        rdbReportCorruptRDB("Duplicate set members detected");
                        decrRefCount(o);
                        sdsfree(sdsele);
                        return NULL;
                    }
                } else {
                    setTypeConvert(o,OBJ_ENCODING_HT);
                    if (dictTryExpand((dict*)ptrFromObj(o),len) != DICT_OK) {
                        rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)len);
                        sdsfree(sdsele);
                        decrRefCount(o);
                        return NULL;
                    }
                }
            }

            /* This will also be called when the set was just converted
             * to a regular hash table encoded set. */
            if (o->encoding == OBJ_ENCODING_HT) {
                if (dictAdd((dict*)ptrFromObj(o),sdsele,NULL) != DICT_OK) {
                    rdbReportCorruptRDB("Duplicate set members detected");
                    decrRefCount(o);
                    sdsfree(sdsele);
                    return NULL;
                }
            } else {
                sdsfree(sdsele);
            }
        }
    } else if (rdbtype == RDB_TYPE_ZSET_2 || rdbtype == RDB_TYPE_ZSET) {
        /* Read list/set value. */
        uint64_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        if ((zsetlen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        o = createZsetObject();
        zs = (zset*)ptrFromObj(o);

        if (zsetlen > DICT_HT_INITIAL_SIZE && dictTryExpand(zs->dict,zsetlen) != DICT_OK) {
            rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)zsetlen);
            decrRefCount(o);
            return NULL;
        }

        /* Load every single element of the sorted set. */
        while(zsetlen--) {
            sds sdsele;
            double score;
            zskiplistNode *znode;

            if ((sdsele = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                decrRefCount(o);
                return NULL;
            }

            if (rdbtype == RDB_TYPE_ZSET_2) {
                if (rdbLoadBinaryDoubleValue(rdb,&score) == -1) {
                    decrRefCount(o);
                    sdsfree(sdsele);
                    return NULL;
                }
            } else {
                if (rdbLoadDoubleValue(rdb,&score) == -1) {
                    decrRefCount(o);
                    sdsfree(sdsele);
                    return NULL;
                }
            }

            /* Don't care about integer-encoded strings. */
            if (sdslen(sdsele) > maxelelen) maxelelen = sdslen(sdsele);

            znode = zslInsert(zs->zsl,score,sdsele);
            if (dictAdd(zs->dict,sdsele,&znode->score) != DICT_OK) {
                rdbReportCorruptRDB("Duplicate zset fields detected");
                decrRefCount(o);
                /* no need to free 'sdsele', will be released by zslFree together with 'o' */
                return NULL;
            }
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. */
        if (zsetLength(o) <= g_pserver->zset_max_ziplist_entries &&
            maxelelen <= g_pserver->zset_max_ziplist_value)
                zsetConvert(o,OBJ_ENCODING_ZIPLIST);
    } else if (rdbtype == RDB_TYPE_HASH) {
        uint64_t len;
        int ret;
        sds field, value;
        dict *dupSearchDict = NULL;

        len = rdbLoadLen(rdb, NULL);
        if (len == RDB_LENERR) return NULL;

        o = createHashObject();

        /* Too many entries? Use a hash table. */
        if (len > g_pserver->hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);
        else if (deep_integrity_validation) {
            /* In this mode, we need to guarantee that the server won't crash
             * later when the ziplist is converted to a dict.
             * Create a set (dict with no values) to for a dup search.
             * We can dismiss it as soon as we convert the ziplist to a hash. */
            dupSearchDict = dictCreate(&hashDictType, NULL);
        }


        /* Load every field and value into the ziplist */
        while (o->encoding == OBJ_ENCODING_ZIPLIST && len > 0) {
            len--;
            /* Load raw strings */
            if ((field = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                decrRefCount(o);
                if (dupSearchDict) dictRelease(dupSearchDict);
                return NULL;
            }
            if ((value = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                sdsfree(field);
                decrRefCount(o);
                if (dupSearchDict) dictRelease(dupSearchDict);
                return NULL;
            }

            if (dupSearchDict) {
                sds field_dup = sdsdup(field);
                if (dictAdd(dupSearchDict, field_dup, NULL) != DICT_OK) {
                    rdbReportCorruptRDB("Hash with dup elements");
                    dictRelease(dupSearchDict);
                    decrRefCount(o);
                    sdsfree(field_dup);
                    sdsfree(field);
                    sdsfree(value);
                    return NULL;
                }
            }

            /* Add pair to ziplist */
            o->m_ptr = ziplistPush((unsigned char*)ptrFromObj(o), (unsigned char*)field,
                    sdslen(field), ZIPLIST_TAIL);
            o->m_ptr = ziplistPush((unsigned char*)ptrFromObj(o), (unsigned char*)value,
                    sdslen(value), ZIPLIST_TAIL);

            /* Convert to hash table if size threshold is exceeded */
            if (sdslen(field) > g_pserver->hash_max_ziplist_value ||
                sdslen(value) > g_pserver->hash_max_ziplist_value)
            {
                sdsfree(field);
                sdsfree(value);
                hashTypeConvert(o, OBJ_ENCODING_HT);
                break;
            }
            sdsfree(field);
            sdsfree(value);
        }

        if (dupSearchDict) {
            /* We no longer need this, from now on the entries are added
             * to a dict so the check is performed implicitly. */
            dictRelease(dupSearchDict);
            dupSearchDict = NULL;
        }

        if (o->encoding == OBJ_ENCODING_HT && len > DICT_HT_INITIAL_SIZE) {
            if (dictTryExpand((dict*)ptrFromObj(o),len) != DICT_OK) {
                rdbReportCorruptRDB("OOM in dictTryExpand %llu", (unsigned long long)len);
                decrRefCount(o);
                return NULL;
            }
        }

        /* Load remaining fields and values into the hash table */
        while (o->encoding == OBJ_ENCODING_HT && len > 0) {
            len--;
            /* Load encoded strings */
            if ((field = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                decrRefCount(o);
                return NULL;
            }
            if ((value = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
                sdsfree(field);
                decrRefCount(o);
                return NULL;
            }

            /* Add pair to hash table */
            ret = dictAdd((dict*)ptrFromObj(o), field, value);
            if (ret == DICT_ERR) {
                rdbReportCorruptRDB("Duplicate hash fields detected");
                sdsfree(value);
                sdsfree(field);
                decrRefCount(o);
                return NULL;
            }
        }

        /* All pairs should be read by now */
        serverAssert(len == 0);
    } else if (rdbtype == RDB_TYPE_LIST_QUICKLIST) {
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        o = createQuicklistObject();
        quicklistSetOptions((quicklist*)ptrFromObj(o), g_pserver->list_max_ziplist_size,
                            g_pserver->list_compress_depth);

        while (len--) {
            size_t encoded_len;
            unsigned char *zl = (unsigned char*)
                rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,&encoded_len);
            if (zl == NULL) {
                decrRefCount(o);
                return NULL;
            }
            if (deep_integrity_validation) g_pserver->stat_dump_payload_sanitizations++;
            if (!ziplistValidateIntegrity(zl, encoded_len, deep_integrity_validation, NULL, NULL)) {
                rdbReportCorruptRDB("Ziplist integrity check failed.");
                decrRefCount(o);
                zfree(zl);
                return NULL;
            }
            quicklistAppendZiplist((quicklist*)ptrFromObj(o), zl);
        }
    } else if (rdbtype == RDB_TYPE_HASH_ZIPMAP  ||
               rdbtype == RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == RDB_TYPE_SET_INTSET   ||
               rdbtype == RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == RDB_TYPE_HASH_ZIPLIST)
    {
        size_t encoded_len;
        unsigned char *encoded = (unsigned char*)
            rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,&encoded_len);
        if (encoded == NULL) return NULL;

        o = createObject(OBJ_STRING,encoded); /* Obj type fixed below. */

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. */
        switch(rdbtype) {
            case RDB_TYPE_HASH_ZIPMAP:
                /* Since we don't keep zipmaps anymore, the rdb loading for these
                 * is O(n) anyway, use `deep` validation. */
                if (!zipmapValidateIntegrity(encoded, encoded_len, 1)) {
                    rdbReportCorruptRDB("Zipmap integrity check failed.");
                    zfree(encoded);
                    o->m_ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
                {
                    unsigned char *zl = ziplistNew();
                    unsigned char *zi = zipmapRewind((unsigned char*)ptrFromObj(o));
                    unsigned char *fstr, *vstr;
                    unsigned int flen, vlen;
                    unsigned int maxlen = 0;
                    dict *dupSearchDict = dictCreate(&hashDictType, NULL);

                    while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                        if (flen > maxlen) maxlen = flen;
                        if (vlen > maxlen) maxlen = vlen;
                        zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
                        zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);

                        /* search for duplicate records */
                        sds field = sdstrynewlen(fstr, flen);
                        if (!field || dictAdd(dupSearchDict, field, NULL) != DICT_OK) {
                            rdbReportCorruptRDB("Hash zipmap with dup elements, or big length (%u)", flen);
                            dictRelease(dupSearchDict);
                            sdsfree(field);
                            zfree(encoded);
                            o->m_ptr = NULL;
                            decrRefCount(o);
                            return NULL;
                        }
                    }

                    dictRelease(dupSearchDict);
                    zfree(ptrFromObj(o));
                    o->m_ptr = zl;
                    o->type = OBJ_HASH;
                    o->encoding = OBJ_ENCODING_ZIPLIST;

                    if (hashTypeLength(o) > g_pserver->hash_max_ziplist_entries ||
                        maxlen > g_pserver->hash_max_ziplist_value)
                    {
                        hashTypeConvert(o, OBJ_ENCODING_HT);
                    }
                }
                break;
            case RDB_TYPE_LIST_ZIPLIST:
                if (deep_integrity_validation) g_pserver->stat_dump_payload_sanitizations++;
                if (!ziplistValidateIntegrity(encoded, encoded_len, deep_integrity_validation, NULL, NULL)) {
                    rdbReportCorruptRDB("List ziplist integrity check failed.");
                    zfree(encoded);
                    o->m_ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                o->type = OBJ_LIST;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                listTypeConvert(o,OBJ_ENCODING_QUICKLIST);
                break;
            case RDB_TYPE_SET_INTSET:
                if (deep_integrity_validation) g_pserver->stat_dump_payload_sanitizations++;
                if (!intsetValidateIntegrity(encoded, encoded_len, deep_integrity_validation)) {
                    rdbReportCorruptRDB("Intset integrity check failed.");
                    zfree(encoded);
                    o->m_ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                o->type = OBJ_SET;
                o->encoding = OBJ_ENCODING_INTSET;
                if (intsetLen((intset*)ptrFromObj(o)) > g_pserver->set_max_intset_entries)
                    setTypeConvert(o,OBJ_ENCODING_HT);
                break;
            case RDB_TYPE_ZSET_ZIPLIST:
                if (deep_integrity_validation) g_pserver->stat_dump_payload_sanitizations++;
                if (!zsetZiplistValidateIntegrity(encoded, encoded_len, deep_integrity_validation)) {
                    rdbReportCorruptRDB("Zset ziplist integrity check failed.");
                    zfree(encoded);
                    o->m_ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                o->type = OBJ_ZSET;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (zsetLength(o) > g_pserver->zset_max_ziplist_entries)
                    zsetConvert(o,OBJ_ENCODING_SKIPLIST);
                break;
            case RDB_TYPE_HASH_ZIPLIST:
                if (deep_integrity_validation) g_pserver->stat_dump_payload_sanitizations++;
                if (!hashZiplistValidateIntegrity(encoded, encoded_len, deep_integrity_validation)) {
                    rdbReportCorruptRDB("Hash ziplist integrity check failed.");
                    zfree(encoded);
                    o->m_ptr = NULL;
                    decrRefCount(o);
                    return NULL;
                }
                o->type = OBJ_HASH;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (hashTypeLength(o) > g_pserver->hash_max_ziplist_entries)
                    hashTypeConvert(o, OBJ_ENCODING_HT);
                break;
            default:
                /* totally unreachable */
                rdbReportCorruptRDB("Unknown RDB encoding type %d",rdbtype);
                break;
        }
    } else if (rdbtype == RDB_TYPE_STREAM_LISTPACKS) {
        o = createStreamObject();
        stream *s = (stream*)ptrFromObj(o);
        uint64_t listpacks = rdbLoadLen(rdb,NULL);
        if (listpacks == RDB_LENERR) {
            rdbReportReadError("Stream listpacks len loading failed.");
            decrRefCount(o);
            return NULL;
        }

        while(listpacks--) {
            /* Get the master ID, the one we'll use as key of the radix tree
             * node: the entries inside the listpack itself are delta-encoded
             * relatively to this ID. */
            sds nodekey = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
            if (nodekey == NULL) {
                rdbReportReadError("Stream master ID loading failed: invalid encoding or I/O error.");
                decrRefCount(o);
                return NULL;
            }
            if (sdslen(nodekey) != sizeof(streamID)) {
                rdbReportCorruptRDB("Stream node key entry is not the "
                                        "size of a stream ID");
                sdsfree(nodekey);
                decrRefCount(o);
                return NULL;
            }

            /* Load the listpack. */
            size_t lp_size;
            unsigned char *lp = (unsigned char*)
                rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,&lp_size);
            if (lp == NULL) {
                rdbReportReadError("Stream listpacks loading failed.");
                sdsfree(nodekey);
                decrRefCount(o);
                return NULL;
            }
            if (deep_integrity_validation) g_pserver->stat_dump_payload_sanitizations++;
            if (!streamValidateListpackIntegrity(lp, lp_size, deep_integrity_validation)) {
                rdbReportCorruptRDB("Stream listpack integrity check failed.");
                sdsfree(nodekey);
                decrRefCount(o);
                zfree(lp);
                return NULL;
            }

            unsigned char *first = lpFirst(lp);
            if (first == NULL) {
                /* Serialized listpacks should never be empty, since on
                 * deletion we should remove the radix tree key if the
                 * resulting listpack is empty. */
                rdbReportCorruptRDB("Empty listpack inside stream");
                sdsfree(nodekey);
                decrRefCount(o);
                zfree(lp);
                return NULL;
            }

            /* Insert the key in the radix tree. */
            int retval = raxInsert(s->rax,
                (unsigned char*)nodekey,sizeof(streamID),lp,NULL);
            sdsfree(nodekey);
            if (!retval) {
                rdbReportCorruptRDB("Listpack re-added with existing key");
                decrRefCount(o);
                zfree(lp);
                return NULL;
            }
        }
        /* Load total number of items inside the stream. */
        s->length = rdbLoadLen(rdb,NULL);

        /* Load the last entry ID. */
        s->last_id.ms = rdbLoadLen(rdb,NULL);
        s->last_id.seq = rdbLoadLen(rdb,NULL);

        if (rioGetReadError(rdb)) {
            rdbReportReadError("Stream object metadata loading failed.");
            decrRefCount(o);
            return NULL;
        }

        /* Consumer groups loading */
        uint64_t cgroups_count = rdbLoadLen(rdb,NULL);
        if (cgroups_count == RDB_LENERR) {
            rdbReportReadError("Stream cgroup count loading failed.");
            decrRefCount(o);
            return NULL;
        }
        while(cgroups_count--) {
            /* Get the consumer group name and ID. We can then create the
             * consumer group ASAP and populate its structure as
             * we read more data. */
            streamID cg_id;
            sds cgname = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
            if (cgname == NULL) {
                rdbReportReadError(
                    "Error reading the consumer group name from Stream");
                decrRefCount(o);
                return NULL;
            }

            cg_id.ms = rdbLoadLen(rdb,NULL);
            cg_id.seq = rdbLoadLen(rdb,NULL);
            if (rioGetReadError(rdb)) {
                rdbReportReadError("Stream cgroup ID loading failed.");
                sdsfree(cgname);
                decrRefCount(o);
                return NULL;
            }

            streamCG *cgroup = streamCreateCG(s,cgname,sdslen(cgname),&cg_id);
            if (cgroup == NULL) {
                rdbReportCorruptRDB("Duplicated consumer group name %s",
                                         cgname);
                decrRefCount(o);
                sdsfree(cgname);
                return NULL;
            }
            sdsfree(cgname);

            /* Load the global PEL for this consumer group, however we'll
             * not yet populate the NACK structures with the message
             * owner, since consumers for this group and their messages will
             * be read as a next step. So for now leave them not resolved
             * and later populate it. */
            uint64_t pel_size = rdbLoadLen(rdb,NULL);
            if (pel_size == RDB_LENERR) {
                rdbReportReadError("Stream PEL size loading failed.");
                decrRefCount(o);
                return NULL;
            }
            while(pel_size--) {
                unsigned char rawid[sizeof(streamID)];
                if (rioRead(rdb,rawid,sizeof(rawid)) == 0) {
                    rdbReportReadError("Stream PEL ID loading failed.");
                    decrRefCount(o);
                    return NULL;
                }
                streamNACK *nack = streamCreateNACK(NULL);
                nack->delivery_time = rdbLoadMillisecondTime(rdb,RDB_VERSION);
                nack->delivery_count = rdbLoadLen(rdb,NULL);
                if (rioGetReadError(rdb)) {
                    rdbReportReadError("Stream PEL NACK loading failed.");
                    decrRefCount(o);
                    streamFreeNACK(nack);
                    return NULL;
                }
                if (!raxInsert(cgroup->pel,rawid,sizeof(rawid),nack,NULL)) {
                    rdbReportCorruptRDB("Duplicated global PEL entry "
                                            "loading stream consumer group");
                    decrRefCount(o);
                    streamFreeNACK(nack);
                    return NULL;
                }
            }

            /* Now that we loaded our global PEL, we need to load the
             * consumers and their local PELs. */
            uint64_t consumers_num = rdbLoadLen(rdb,NULL);
            if (consumers_num == RDB_LENERR) {
                rdbReportReadError("Stream consumers num loading failed.");
                decrRefCount(o);
                return NULL;
            }
            while(consumers_num--) {
                sds cname = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
                if (cname == NULL) {
                    rdbReportReadError(
                        "Error reading the consumer name from Stream group.");
                    decrRefCount(o);
                    return NULL;
                }
                streamConsumer *consumer =
                    streamLookupConsumer(cgroup,cname,SLC_NONE,NULL);
                sdsfree(cname);
                consumer->seen_time = rdbLoadMillisecondTime(rdb,RDB_VERSION);
                if (rioGetReadError(rdb)) {
                    rdbReportReadError("Stream short read reading seen time.");
                    decrRefCount(o);
                    return NULL;
                }

                /* Load the PEL about entries owned by this specific
                 * consumer. */
                pel_size = rdbLoadLen(rdb,NULL);
                if (pel_size == RDB_LENERR) {
                    rdbReportReadError(
                        "Stream consumer PEL num loading failed.");
                    decrRefCount(o);
                    return NULL;
                }
                while(pel_size--) {
                    unsigned char rawid[sizeof(streamID)];
                    if (rioRead(rdb,rawid,sizeof(rawid)) == 0) {
                        rdbReportReadError(
                            "Stream short read reading PEL streamID.");
                        decrRefCount(o);
                        return NULL;
                    }
                    streamNACK *nack = (streamNACK*)raxFind(cgroup->pel,rawid,sizeof(rawid));
                    if (nack == raxNotFound) {
                        rdbReportCorruptRDB("Consumer entry not found in "
                                                "group global PEL");
                        decrRefCount(o);
                        return NULL;
                    }

                    /* Set the NACK consumer, that was left to NULL when
                     * loading the global PEL. Then set the same shared
                     * NACK structure also in the consumer-specific PEL. */
                    nack->consumer = consumer;
                    if (!raxInsert(consumer->pel,rawid,sizeof(rawid),nack,NULL)) {
                        rdbReportCorruptRDB("Duplicated consumer PEL entry "
                                                " loading a stream consumer "
                                                "group");
                        decrRefCount(o);
                        return NULL;
                    }
                }
            }
        }
    } else if (rdbtype == RDB_TYPE_MODULE || rdbtype == RDB_TYPE_MODULE_2) {
        uint64_t moduleid = rdbLoadLen(rdb,NULL);
        if (rioGetReadError(rdb)) {
            rdbReportReadError("Short read module id");
            return NULL;
        }
        moduleType *mt = moduleTypeLookupModuleByID(moduleid);

        if (rdbCheckMode && rdbtype == RDB_TYPE_MODULE_2) {
            char name[10];
            moduleTypeNameByID(name,moduleid);
            return rdbLoadCheckModuleValue(rdb,name);
        }

        if (mt == NULL) {
            char name[10];
            moduleTypeNameByID(name,moduleid);
            rdbReportCorruptRDB("The RDB file contains module data I can't load: no matching module type '%s'", name);
            return NULL;
        }
        RedisModuleIO io;
        redisObjectStack keyobj;
        initStaticStringObject(keyobj,key);
        moduleInitIOContext(io,mt,rdb,&keyobj);
        io.ver = (rdbtype == RDB_TYPE_MODULE) ? 1 : 2;
        /* Call the rdb_load method of the module providing the 10 bit
         * encoding version in the lower 10 bits of the module ID. */
        void *ptr = mt->rdb_load(&io,moduleid&1023);
        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }

        /* Module v2 serialization has an EOF mark at the end. */
        if (io.ver == 2) {
            uint64_t eof = rdbLoadLen(rdb,NULL);
            if (eof == RDB_LENERR) {
                if (ptr) {
                    o = createModuleObject(mt,ptr); /* creating just in order to easily destroy */
                    decrRefCount(o);
                }
                return NULL;
            }
            if (eof != RDB_MODULE_OPCODE_EOF) {
                rdbReportCorruptRDB("The RDB file contains module data for the module '%s' that is not terminated by "
                                    "the proper module value EOF marker", moduleTypeModuleName(mt));
                if (ptr) {
                    o = createModuleObject(mt,ptr); /* creating just in order to easily destroy */
                    decrRefCount(o);
                }
                return NULL;
            }
        }

        if (ptr == NULL) {
            rdbReportCorruptRDB("The RDB file contains module data for the module type '%s', that the responsible "
                                "module is not able to load. Check for modules log above for additional clues.",
                                moduleTypeModuleName(mt));
            return NULL;
        }
        o = createModuleObject(mt,ptr);
    } else if (rdbtype == RDB_TYPE_CRON) {
        std::unique_ptr<cronjob> spjob = std::make_unique<cronjob>();
        spjob->script = rdbLoadString(rdb);
        spjob->startTime = rdbLoadMillisecondTime(rdb,RDB_VERSION);
        spjob->interval = rdbLoadMillisecondTime(rdb,RDB_VERSION);
        auto ckeys = rdbLoadLen(rdb,NULL);
        for (uint64_t i = 0; i < ckeys; ++i)
            spjob->veckeys.push_back(rdbLoadString(rdb));
        auto cargs = rdbLoadLen(rdb,NULL);
        for (uint64_t i = 0; i < cargs; ++i)
            spjob->vecargs.push_back(rdbLoadString(rdb));
        o = createObject(OBJ_CRON, spjob.release());
    } else {
        rdbReportReadError("Unknown RDB encoding type %d",rdbtype);
        return NULL;
    }

    setMvccTstamp(o, mvcc_tstamp);
    serverAssert(!o->FExpires());
    return o;
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats. */
void startLoading(size_t size, int rdbflags) {
    /* Load the DB */
    g_pserver->loading = (rdbflags & RDBFLAGS_REPLICATION) ? LOADING_REPLICATION : LOADING_BOOT;
    g_pserver->loading_start_time = time(NULL);
    g_pserver->loading_loaded_bytes = 0;
    g_pserver->loading_total_bytes = size;
    g_pserver->loading_rdb_used_mem = 0;
    blockingOperationStarts();

    /* Fire the loading modules start event. */
    int subevent;
    if (rdbflags & RDBFLAGS_AOF_PREAMBLE)
        subevent = REDISMODULE_SUBEVENT_LOADING_AOF_START;
    else if(rdbflags & RDBFLAGS_REPLICATION)
        subevent = REDISMODULE_SUBEVENT_LOADING_REPL_START;
    else
        subevent = REDISMODULE_SUBEVENT_LOADING_RDB_START;
    moduleFireServerEvent(REDISMODULE_EVENT_LOADING,subevent,NULL);
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats.
 * 'filename' is optional and used for rdb-check on error */
void startLoadingFile(FILE *fp, const char* filename, int rdbflags) {
    struct stat sb;
    if (fstat(fileno(fp), &sb) == -1)
        sb.st_size = 0;
    rdbFileBeingLoaded = filename;
    startLoading(sb.st_size, rdbflags);
}

/* Refresh the loading progress info */
void loadingProgress(off_t pos) {
    g_pserver->loading_loaded_bytes = pos;
    if (g_pserver->stat_peak_memory < zmalloc_used_memory())
        g_pserver->stat_peak_memory = zmalloc_used_memory();
}

/* Loading finished */
void stopLoading(int success) {
    g_pserver->loading = 0;
    blockingOperationEnds();
    rdbFileBeingLoaded = NULL;

    /* Fire the loading modules end event. */
    moduleFireServerEvent(REDISMODULE_EVENT_LOADING,
                          success?
                            REDISMODULE_SUBEVENT_LOADING_ENDED:
                            REDISMODULE_SUBEVENT_LOADING_FAILED,
                          NULL);
}

void startSaving(int rdbflags) {
    /* Fire the persistence modules end event. */
    int subevent;
    if (rdbflags & RDBFLAGS_AOF_PREAMBLE)
        subevent = REDISMODULE_SUBEVENT_PERSISTENCE_AOF_START;
    else if (getpid()!=cserver.pid)
        subevent = REDISMODULE_SUBEVENT_PERSISTENCE_RDB_START;
    else
        subevent = REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START;
    moduleFireServerEvent(REDISMODULE_EVENT_PERSISTENCE,subevent,NULL);
}

void stopSaving(int success) {
    /* Fire the persistence modules end event. */
    moduleFireServerEvent(REDISMODULE_EVENT_PERSISTENCE,
                          success?
                            REDISMODULE_SUBEVENT_PERSISTENCE_ENDED:
                            REDISMODULE_SUBEVENT_PERSISTENCE_FAILED,
                          NULL);
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate rdb checksum  */
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    if (g_pserver->rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    
    if ((g_pserver->loading_process_events_interval_bytes &&
        (r->processed_bytes + len)/g_pserver->loading_process_events_interval_bytes > r->processed_bytes/g_pserver->loading_process_events_interval_bytes) ||
        (g_pserver->loading_process_events_interval_keys &&
        (r->keys_since_last_callback >= g_pserver->loading_process_events_interval_keys)))
    {
        listIter li;
        listNode *ln;
        listRewind(g_pserver->masters, &li);
        while ((ln = listNext(&li)))
        {
            struct redisMaster *mi = (struct redisMaster*)listNodeValue(ln);
            if (mi->masterhost && mi->repl_state == REPL_STATE_TRANSFER)
                replicationSendNewlineToMaster(mi);
        }
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked(serverTL - g_pserver->rgthreadvar);
        processModuleLoadingProgressEvent(0);

        robj *ping_argv[1];

        ping_argv[0] = createStringObject("PING",4);
        replicationFeedSlaves(g_pserver->slaves, g_pserver->replicaseldb, ping_argv, 1);
        decrRefCount(ping_argv[0]);

        r->keys_since_last_callback = 0;
    }
}

/* Load an RDB file from the rio stream 'rdb'. On success C_OK is returned,
 * otherwise C_ERR is returned and 'errno' is set accordingly. */
int rdbLoadRio(rio *rdb, int rdbflags, rdbSaveInfo *rsi) {
    uint64_t dbid;
    int type, rdbver;
    redisDb *db = g_pserver->db+0;
    char buf[1024];
    /* Key-specific attributes, set by opcodes before the key type. */
    long long lru_idle = -1, lfu_freq = -1, expiretime = -1, now;
    long long lru_clock = 0;
    uint64_t mvcc_tstamp = OBJ_MVCC_INVALID;
    robj *subexpireKey = nullptr;
    sds key = nullptr;
    bool fLastKeyExpired = false;

    rdb->update_cksum = rdbLoadProgressCallback;
    rdb->max_processing_chunk = g_pserver->loading_process_events_interval_bytes;
    if (rioRead(rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        serverLog(LL_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return C_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        serverLog(LL_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return C_ERR;
    }

    now = mstime();
    lru_clock = LRU_CLOCK();
    
    while(1) {
        robj *val;

        /* Read type. */
        if ((type = rdbLoadType(rdb)) == -1) goto eoferr;

        /* Handle special types. */
        if (type == RDB_OPCODE_EXPIRETIME) {
            /* EXPIRETIME: load an expire associated with the next key
             * to load. Note that after loading an expire we need to
             * load the actual type, and continue. */
            expiretime = rdbLoadTime(rdb);
            expiretime *= 1000;
            if (rioGetReadError(rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: milliseconds precision expire times introduced
             * with RDB v3. Like EXPIRETIME but no with more precision. */
            expiretime = rdbLoadMillisecondTime(rdb,rdbver);
            if (rioGetReadError(rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_FREQ) {
            /* FREQ: LFU frequency. */
            uint8_t byte;
            if (rioRead(rdb,&byte,1) == 0) goto eoferr;
            lfu_freq = byte;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_IDLE) {
            /* IDLE: LRU idle time. */
            uint64_t qword;
            if ((qword = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto eoferr;
            lru_idle = qword;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EOF) {
            /* EOF: End of file, exit the main loop. */
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB: Select the specified database. */
            if ((dbid = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto eoferr;
            if (dbid >= (unsigned)cserver.dbnum) {
                serverLog(LL_WARNING,
                    "FATAL: Data file was created with a KeyDB "
                    "server configured to handle more than %d "
                    "databases. Exiting\n", cserver.dbnum);
                exit(1);
            }
            db = g_pserver->db+dbid;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            /* RESIZEDB: Hint about the size of the keys in the currently
             * selected data base, in order to avoid useless rehashing. */
            uint64_t db_size, expires_size;
            if ((db_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            dictExpand(db->dict,db_size);
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_AUX) {
            /* AUX: generic string-string fields. Use to add state to RDB
             * which is backward compatible. Implementations of RDB loading
             * are required to skip AUX fields they don't understand.
             *
             * An AUX field is composed of two strings: key and value. */
            robj *auxkey, *auxval;
            if ((auxkey = rdbLoadStringObject(rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(rdb)) == NULL) goto eoferr;

            if (((char*)ptrFromObj(auxkey))[0] == '%') {
                /* All the fields with a name staring with '%' are considered
                 * information fields and are logged at startup with a log
                 * level of NOTICE. */
                serverLog(LL_NOTICE,"RDB '%s': %s",
                    (char*)ptrFromObj(auxkey),
                    (char*)ptrFromObj(auxval));
            } else if (!strcasecmp(szFromObj(auxkey),"repl-stream-db")) {
                if (rsi) rsi->repl_stream_db = atoi(szFromObj(auxval));
            } else if (!strcasecmp(szFromObj(auxkey),"repl-id")) {
                if (rsi && sdslen(szFromObj(auxval)) == CONFIG_RUN_ID_SIZE) {
                    memcpy(rsi->repl_id,ptrFromObj(auxval),CONFIG_RUN_ID_SIZE+1);
                    rsi->repl_id_is_set = 1;
                }
            } else if (!strcasecmp(szFromObj(auxkey),"repl-offset")) {
                if (rsi) rsi->repl_offset = strtoll(szFromObj(auxval),NULL,10);
            } else if (!strcasecmp(szFromObj(auxkey),"lua")) {
                /* Load the script back in memory. */
                if (luaCreateFunction(NULL,g_pserver->lua,auxval) == NULL) {
                    rdbReportCorruptRDB(
                        "Can't load Lua script from RDB file! "
                        "BODY: %s", (char*)ptrFromObj(auxval));
                }
            } else if (!strcasecmp(szFromObj(auxkey),"redis-ver")) {
                serverLog(LL_NOTICE,"Loading RDB produced by version %s",
                    (const char*)ptrFromObj(auxval));
            } else if (!strcasecmp(szFromObj(auxkey),"ctime")) {
                time_t age = time(NULL)-strtol(szFromObj(auxval),NULL,10);
                if (age < 0) age = 0;
                serverLog(LL_NOTICE,"RDB age %ld seconds",
                    (unsigned long) age);
            } else if (!strcasecmp(szFromObj(auxkey),"used-mem")) {
                long long usedmem = strtoll(szFromObj(auxval),NULL,10);
                serverLog(LL_NOTICE,"RDB memory usage when created %.2f Mb",
                    (double) usedmem / (1024*1024));
                g_pserver->loading_rdb_used_mem = usedmem;
            } else if (!strcasecmp(szFromObj(auxkey),"aof-preamble")) {
                long long haspreamble = strtoll(szFromObj(auxval),NULL,10);
                if (haspreamble) serverLog(LL_NOTICE,"RDB has an AOF tail");
            } else if (!strcasecmp(szFromObj(auxkey),"redis-bits")) {
                /* Just ignored. */
            } else if (!strcasecmp(szFromObj(auxkey),"mvcc-tstamp")) {
                static_assert(sizeof(unsigned long long) == sizeof(uint64_t), "Ensure long long is 64-bits");
                mvcc_tstamp = strtoull(szFromObj(auxval), nullptr, 10);
            } else if (!strcasecmp(szFromObj(auxkey), "keydb-subexpire-key")) {
                if (subexpireKey != nullptr) {
                    serverLog(LL_WARNING, "Corrupt subexpire entry in RDB skipping. key: %s subkey: %s", key != nullptr ? key : "(null)", subexpireKey != nullptr ? szFromObj(subexpireKey) : "(null)");
                    decrRefCount(subexpireKey);
                    subexpireKey = nullptr;
                }
                subexpireKey = auxval;
                incrRefCount(subexpireKey);
            } else if (!strcasecmp(szFromObj(auxkey), "keydb-subexpire-when")) {
                if (key == nullptr || subexpireKey == nullptr) {
                    if (!fLastKeyExpired) { // This is not an error if we just expired the key associated with this subexpire
                        serverLog(LL_WARNING, "Corrupt subexpire entry in RDB skipping. key: %s subkey: %s", key != nullptr ? key : "(null)", subexpireKey != nullptr ? szFromObj(subexpireKey) : "(null)");
                    }
                    if (subexpireKey) {
                        decrRefCount(subexpireKey);
                        subexpireKey = nullptr;
                    }
                }
                else {
                    redisObjectStack keyobj;
                    initStaticStringObject(keyobj,key);
                    long long expireT = strtoll(szFromObj(auxval), nullptr, 10);
                    setExpire(NULL, db, &keyobj, subexpireKey, expireT);
                    replicateSubkeyExpire(db, &keyobj, subexpireKey, expireT);
                    decrRefCount(subexpireKey);
                    subexpireKey = nullptr;
                }
            } else {
                /* We ignore fields we don't understand, as by AUX field
                 * contract. */
                serverLog(LL_DEBUG,"Unrecognized RDB AUX field: '%s'",
                    (char*)ptrFromObj(auxkey));
            }

            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_MODULE_AUX) {
            /* Load module data that is not related to the Redis key space.
             * Such data can be potentially be stored both before and after the
             * RDB keys-values section. */
            uint64_t moduleid = rdbLoadLen(rdb,NULL);
            int when_opcode = rdbLoadLen(rdb,NULL);
            int when = rdbLoadLen(rdb,NULL);
            if (rioGetReadError(rdb)) goto eoferr;
            if (when_opcode != RDB_MODULE_OPCODE_UINT)
                rdbReportReadError("bad when_opcode");
            moduleType *mt = moduleTypeLookupModuleByID(moduleid);
            char name[10];
            moduleTypeNameByID(name,moduleid);

            if (!rdbCheckMode && mt == NULL) {
                /* Unknown module. */
                serverLog(LL_WARNING,"The RDB file contains AUX module data I can't load: no matching module '%s'", name);
                exit(1);
            } else if (!rdbCheckMode && mt != NULL) {
                if (!mt->aux_load) {
                    /* Module doesn't support AUX. */
                    serverLog(LL_WARNING,"The RDB file contains module AUX data, but the module '%s' doesn't seem to support it.", name);
                    exit(1);
                }

                RedisModuleIO io;
                moduleInitIOContext(io,mt,rdb,NULL);
                io.ver = 2;
                /* Call the rdb_load method of the module providing the 10 bit
                 * encoding version in the lower 10 bits of the module ID. */
                if (mt->aux_load(&io,moduleid&1023, when) != REDISMODULE_OK || io.error) {
                    moduleTypeNameByID(name,moduleid);
                    serverLog(LL_WARNING,"The RDB file contains module AUX data for the module type '%s', that the responsible module is not able to load. Check for modules log above for additional clues.", name);
                    exit(1);
                }
                if (io.ctx) {
                    moduleFreeContext(io.ctx);
                    zfree(io.ctx);
                }
                uint64_t eof = rdbLoadLen(rdb,NULL);
                if (eof != RDB_MODULE_OPCODE_EOF) {
                    serverLog(LL_WARNING,"The RDB file contains module AUX data for the module '%s' that is not terminated by the proper module value EOF marker", name);
                    exit(1);
                }
                continue;
            } else {
                /* RDB check mode. */
                robj *aux = rdbLoadCheckModuleValue(rdb,name);
                decrRefCount(aux);
                continue; /* Read next opcode. */
            }
        }

        /* Read key */
        if (key != nullptr)
        {
            sdsfree(key);
            key = nullptr;
        }

        if ((key = (sds)rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL)
            goto eoferr;
        /* Read value */
        if ((val = rdbLoadObject(type,rdb,key,mvcc_tstamp)) == NULL) {
            sdsfree(key);
            key = nullptr;
            goto eoferr;
        }
        bool fStaleMvccKey = (rsi) ? mvccFromObj(val) < rsi->mvccMinThreshold : false;

        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the replica.
         * Similarly if the RDB is the preamble of an AOF file, we want to
         * load all the keys as they are, since the log of operations later
         * assume to work in an exact keyspace state. */
        redisObjectStack keyobj;
        initStaticStringObject(keyobj,key);
        bool fExpiredKey = iAmMaster() && !(rdbflags&RDBFLAGS_AOF_PREAMBLE) && expiretime != -1 && expiretime < now;
        if (fStaleMvccKey || fExpiredKey) {
            if (fStaleMvccKey && !fExpiredKey && rsi != nullptr && rsi->mi != nullptr && rsi->mi->staleKeyMap != nullptr && lookupKeyRead(db, &keyobj) == nullptr) {
                // We have a key that we've already deleted and is not back in our database.
                //  We'll need to inform the sending master of the delete if it is also a replica of us
                robj_sharedptr objKeyDup(createStringObject(key, sdslen(key)));
                rsi->mi->staleKeyMap->operator[](db - g_pserver->db).push_back(objKeyDup);                
            }
            fLastKeyExpired = true;
            sdsfree(key);
            key = nullptr;
            decrRefCount(val);
            val = nullptr;
        } else {
            redisObjectStack keyobj;
            initStaticStringObject(keyobj,key);

            /* Add the new object in the hash table */
            int fInserted = dbMerge(db, key, val, (rsi && rsi->fForceSetKey) || (rdbflags & RDBFLAGS_ALLOW_DUP));   // Note: dbMerge will incrRef
            fLastKeyExpired = false;

            if (fInserted)
            {
                /* Set the expire time if needed */
                if (expiretime != -1)
                {
                    setExpire(NULL,db,&keyobj,nullptr,expiretime);
                }

                /* Set usage information (for eviction). */
                objectSetLRUOrLFU(val,lfu_freq,lru_idle,lru_clock,1000);

                /* call key space notification on key loaded for modules only */
                moduleNotifyKeyspaceEvent(NOTIFY_LOADED, "loaded", &keyobj, db->id);

                replicationNotifyLoadedKey(db, &keyobj, val, expiretime);
            }
            else
            {
                decrRefCount(val);
                val = nullptr;
            }
        }

        if (g_pserver->key_load_delay)
            debugDelay(g_pserver->key_load_delay);

        rdb->keys_since_last_callback++;

        /* Reset the state that is key-specified and is populated by
         * opcodes before the key, so that we start from scratch again. */
        expiretime = -1;
        lfu_freq = -1;
        lru_idle = -1;
    }

    if (key != nullptr)
    {
        sdsfree(key);
        key = nullptr;
    }

    if (subexpireKey != nullptr)
    {
        serverLog(LL_WARNING, "Corrupt subexpire entry in RDB.");
        decrRefCount(subexpireKey);
        subexpireKey = nullptr;
    }
    
    /* Verify the checksum if RDB version is >= 5 */
    if (rdbver >= 5) {
        uint64_t cksum, expected = rdb->cksum;

        if (rioRead(rdb,&cksum,8) == 0) goto eoferr;
        if (g_pserver->rdb_checksum && !cserver.skip_checksum_validation) {
            memrev64ifbe(&cksum);
            if (cksum == 0) {
                serverLog(LL_WARNING,"RDB file was saved with checksum disabled: no check performed.");
            } else if (cksum != expected) {
                serverLog(LL_WARNING,"Wrong RDB checksum expected: (%llx) but "
                    "got (%llx). Aborting now.",
                        (unsigned long long)expected,
                        (unsigned long long)cksum);
                rdbReportCorruptRDB("RDB CRC error");
            }
        }
    }
    return C_OK;

    /* Unexpected end of file is handled here calling rdbReportReadError():
     * this will in turn either abort Redis in most cases, or if we are loading
     * the RDB file from a socket during initial SYNC (diskless replica mode),
     * we'll report the error to the caller, so that we can retry. */
eoferr:
    if (key != nullptr)
    {
        sdsfree(key);
        key = nullptr;
    }
    if (subexpireKey != nullptr)
    {
        decrRefCount(subexpireKey);
        subexpireKey = nullptr;
    }

    serverLog(LL_WARNING,
        "Short read or OOM loading DB. Unrecoverable error, aborting now.");
    rdbReportReadError("Unexpected EOF reading RDB file");
    return C_ERR;
}

int rdbLoad(rdbSaveInfo *rsi, int rdbflags)
{
    int err = C_ERR;
    if (g_pserver->rdb_filename != NULL)
        err = rdbLoadFile(g_pserver->rdb_filename, rsi,rdbflags);

    if ((err == C_ERR) && g_pserver->rdb_s3bucketpath != NULL)
        err = rdbLoadS3(g_pserver->rdb_s3bucketpath, rsi, rdbflags);

    return err;
}

/* Like rdbLoadRio() but takes a filename instead of a rio stream. The
 * filename is open for reading and a rio stream object created in order
 * to do the actual loading. Moreover the ETA displayed in the INFO
 * output is initialized and finalized.
 *
 * If you pass an 'rsi' structure initialied with RDB_SAVE_OPTION_INIT, the
 * loading code will fiil the information fields in the structure. */
int rdbLoadFile(const char *filename, rdbSaveInfo *rsi, int rdbflags) {
    FILE *fp;
    rio rdb;
    int retval;

    if ((fp = fopen(filename,"r")) == NULL) return C_ERR;
    startLoadingFile(fp, filename,rdbflags);
    rioInitWithFile(&rdb,fp);
    retval = rdbLoadRio(&rdb,rdbflags,rsi);
    fclose(fp);
    stopLoading(retval==C_OK);
    return retval;
}

/* A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of actual BGSAVEs. */
static void backgroundSaveDoneHandlerDisk(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background saving terminated with success");
        g_pserver->dirty = g_pserver->dirty - g_pserver->dirty_before_bgsave;
        g_pserver->lastsave = time(NULL);
        g_pserver->lastbgsave_status = C_OK;
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background saving error");
        g_pserver->lastbgsave_status = C_ERR;
    } else {
        mstime_t latency;

        serverLog(LL_WARNING,
            "Background saving terminated by signal %d", bysignal);
        latencyStartMonitor(latency);
        rdbRemoveTempFile(g_pserver->child_pid, 0);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("rdb-unlink-temp-file",latency);
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * triggering an error condition. */
        if (bysignal != SIGUSR1)
            g_pserver->lastbgsave_status = C_ERR;
    }
}

/* A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of RDB -> Slaves socket transfers for
 * diskless replication. */
static void backgroundSaveDoneHandlerSocket(int exitcode, int bysignal) {
    serverAssert(GlobalLocksAcquired());

    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background RDB transfer terminated with success");
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background transfer error");
    } else {
        serverLog(LL_WARNING,
            "Background transfer terminated by signal %d", bysignal);
    }
    if (g_pserver->rdb_child_exit_pipe!=-1)
        close(g_pserver->rdb_child_exit_pipe);
    auto pipeT = g_pserver->rdb_pipe_read;
    aePostFunction(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el, [pipeT]{
        aeDeleteFileEvent(serverTL->el, pipeT, AE_READABLE);
        close(pipeT);
    });
    g_pserver->rdb_child_exit_pipe = -1;
    g_pserver->rdb_pipe_read = -1;
    zfree(g_pserver->rdb_pipe_conns);
    g_pserver->rdb_pipe_conns = NULL;
    g_pserver->rdb_pipe_numconns = 0;
    g_pserver->rdb_pipe_numconns_writing = 0;
    zfree(g_pserver->rdb_pipe_buff);
    g_pserver->rdb_pipe_buff = NULL;
    g_pserver->rdb_pipe_bufflen = 0;
}

/* When a background RDB saving/transfer terminates, call the right handler. */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    int type = g_pserver->rdb_child_type;
    switch(g_pserver->rdb_child_type) {
    case RDB_CHILD_TYPE_DISK:
        backgroundSaveDoneHandlerDisk(exitcode,bysignal);
        break;
    case RDB_CHILD_TYPE_SOCKET:
        backgroundSaveDoneHandlerSocket(exitcode,bysignal);
        break;
    default:
        serverPanic("Unknown RDB child type.");
        break;
    }

    g_pserver->rdb_child_type = RDB_CHILD_TYPE_NONE;
    g_pserver->rdb_save_time_last = time(NULL)-g_pserver->rdb_save_time_start;
    g_pserver->rdb_save_time_start = -1;
    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, type);
}

/* Kill the RDB saving child using SIGUSR1 (so that the parent will know
 * the child did not exit for an error, but because we wanted), and performs
 * the cleanup needed. */
void killRDBChild(void) {
    kill(g_pserver->child_pid, SIGUSR1);
    /* Because we are not using here waitpid (like we have in killAppendOnlyChild
     * and TerminateModuleForkChild), all the cleanup operations is done by
     * checkChildrenDone, that later will find that the process killed.
     * This includes:
     * - resetChildState
     * - rdbRemoveTempFile */
}

/* Spawn an RDB child that writes the RDB to the sockets of the slaves
 * that are currently in SLAVE_STATE_WAIT_BGSAVE_START state. */
int rdbSaveToSlavesSockets(rdbSaveInfo *rsi) {
    serverAssert(GlobalLocksAcquired());
    listNode *ln;
    listIter li;
    pid_t childpid;
    int pipefds[2], rdb_pipe_write, safe_to_exit_pipe;

    if (hasActiveChildProcess()) return C_ERR;

    /* Even if the previous fork child exited, don't start a new one until we
     * drained the pipe. */
    if (g_pserver->rdb_pipe_conns) return C_ERR;

    /* Before to fork, create a pipe that is used to transfer the rdb bytes to
     * the parent, we can't let it write directly to the sockets, since in case
     * of TLS we must let the parent handle a continuous TLS state when the
     * child terminates and parent takes over. */
    if (pipe(pipefds) == -1) return C_ERR;
    g_pserver->rdb_pipe_read = pipefds[0]; /* read end */
    rdb_pipe_write = pipefds[1]; /* write end */
    anetNonBlock(NULL, g_pserver->rdb_pipe_read);

    /* create another pipe that is used by the parent to signal to the child
     * that it can exit. */
    if (pipe(pipefds) == -1) {
        close(rdb_pipe_write);
        close(g_pserver->rdb_pipe_read);
        return C_ERR;
    }
    safe_to_exit_pipe = pipefds[0]; /* read end */
    g_pserver->rdb_child_exit_pipe = pipefds[1]; /* write end */

    /* Collect the connections of the replicas we want to transfer
     * the RDB to, which are i WAIT_BGSAVE_START state. */
    g_pserver->rdb_pipe_conns = (connection**)zmalloc(sizeof(connection *)*listLength(g_pserver->slaves));
    g_pserver->rdb_pipe_numconns = 0;
    g_pserver->rdb_pipe_numconns_writing = 0;
    listRewind(g_pserver->slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = (client*)ln->value;
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
            g_pserver->rdb_pipe_conns[g_pserver->rdb_pipe_numconns++] = slave->conn;
            replicationSetupSlaveForFullResync(slave,getPsyncInitialOffset());
        }
    }

    /* Create the child process. */
    if ((childpid = redisFork(CHILD_TYPE_RDB)) == 0) {
        /* Child */
        int retval, dummy;
        rio rdb;

        rioInitWithFd(&rdb,rdb_pipe_write);

        redisSetProcTitle("keydb-rdb-to-slaves");
        redisSetCpuAffinity(g_pserver->bgsave_cpulist);

        retval = rdbSaveRioWithEOFMark(&rdb,NULL,rsi);
        if (retval == C_OK && rioFlush(&rdb) == 0)
            retval = C_ERR;

        if (retval == C_OK) {
            sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
        }

        rioFreeFd(&rdb);
        /* wake up the reader, tell it we're done. */
        close(rdb_pipe_write);
        close(g_pserver->rdb_child_exit_pipe); /* close write end so that we can detect the close on the parent. */
        /* hold exit until the parent tells us it's safe. we're not expecting
         * to read anything, just get the error when the pipe is closed. */
        dummy = read(safe_to_exit_pipe, pipefds, 1);
        UNUSED(dummy);
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        close(safe_to_exit_pipe);
        if (childpid == -1) {
            serverLog(LL_WARNING,"Can't save in background: fork: %s",
                strerror(errno));

            /* Undo the state change. The caller will perform cleanup on
             * all the slaves in BGSAVE_START state, but an early call to
             * replicationSetupSlaveForFullResync() turned it into BGSAVE_END */
            listRewind(g_pserver->slaves,&li);
            while((ln = listNext(&li))) {
                client *replica = (client*)ln->value;
                if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
                    replica->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
                }
            }
            close(rdb_pipe_write);
            close(g_pserver->rdb_pipe_read);
            zfree(g_pserver->rdb_pipe_conns);
            g_pserver->rdb_pipe_conns = NULL;
            g_pserver->rdb_pipe_numconns = 0;
            g_pserver->rdb_pipe_numconns_writing = 0;
        } else {
            serverLog(LL_NOTICE,"Background RDB transfer started by pid %ld",
                (long)childpid);
            g_pserver->rdb_save_time_start = time(NULL);
            g_pserver->rdb_child_type = RDB_CHILD_TYPE_SOCKET;
            updateDictResizePolicy();
            close(rdb_pipe_write); /* close write in parent so that it can detect the close on the child. */
            aePostFunction(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el, []{
                if (aeCreateFileEvent(serverTL->el, g_pserver->rdb_pipe_read, AE_READABLE, rdbPipeReadHandler,NULL) == AE_ERR) {
                    serverPanic("Unrecoverable error creating g_pserver->rdb_pipe_read file event.");
                }
            });
        }
        return (childpid == -1) ? C_ERR : C_OK;
    }
    return C_OK; /* Unreached. */
}

void saveCommand(client *c) {
    if (g_pserver->child_type == CHILD_TYPE_RDB) {
        addReplyError(c,"Background save already in progress");
        return;
    }
    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    if (rdbSave(rsiptr) == C_OK) {
        addReply(c,shared.ok);
    } else {
        addReplyErrorObject(c,shared.err);
    }
}

/* BGSAVE [SCHEDULE] */
void bgsaveCommand(client *c) {
    int schedule = 0;

    /* The SCHEDULE option changes the behavior of BGSAVE when an AOF rewrite
     * is in progress. Instead of returning an error a BGSAVE gets scheduled. */
    if (c->argc > 1) {
        if (c->argc == 2 && !strcasecmp(szFromObj(c->argv[1]),"schedule")) {
            schedule = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);

    if (g_pserver->child_type == CHILD_TYPE_RDB) {
        addReplyError(c,"Background save already in progress");
    } else if (hasActiveChildProcess()) {
        if (schedule) {
            g_pserver->rdb_bgsave_scheduled = 1;
            addReplyStatus(c,"Background saving scheduled");
        } else {
            addReplyError(c,
            "Another child process is active (AOF?): can't BGSAVE right now. "
            "Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever "
            "possible.");
        }
    } else if (rdbSaveBackground(rsiptr) == C_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReplyErrorObject(c,shared.err);
    }
}

/* Populate the rdbSaveInfo structure used to persist the replication
 * information inside the RDB file. Currently the structure explicitly
 * contains just the currently selected DB from the master stream, however
 * if the rdbSave*() family functions receive a NULL rsi structure also
 * the Replication ID/offset is not saved. The function popultes 'rsi'
 * that is normally stack-allocated in the caller, returns the populated
 * pointer if the instance has a valid master client, otherwise NULL
 * is returned, and the RDB saving will not persist any replication related
 * information. */
rdbSaveInfo *rdbPopulateSaveInfo(rdbSaveInfo *rsi) {
    rdbSaveInfo rsi_init = RDB_SAVE_INFO_INIT;
    *rsi = rsi_init;

    /* If the instance is a master, we can populate the replication info
     * only when repl_backlog is not NULL. If the repl_backlog is NULL,
     * it means that the instance isn't in any replication chains. In this
     * scenario the replication info is useless, because when a replica
     * connects to us, the NULL repl_backlog will trigger a full
     * synchronization, at the same time we will use a new replid and clear
     * replid2. */
    if (g_pserver->fActiveReplica || (!listLength(g_pserver->masters) && g_pserver->repl_backlog)) {
        /* Note that when g_pserver->replicaseldb is -1, it means that this master
         * didn't apply any write commands after a full synchronization.
         * So we can let repl_stream_db be 0, this allows a restarted replica
         * to reload replication ID/offset, it's safe because the next write
         * command must generate a SELECT statement. */
        rsi->repl_stream_db = g_pserver->replicaseldb == -1 ? 0 : g_pserver->replicaseldb;
        return rsi;
    }

    if (listLength(g_pserver->masters) > 1)
    {
        // BUGBUG, warn user about this incomplete implementation
        serverLog(LL_WARNING, "Warning: Only backing up first master's information in RDB");
    }
    struct redisMaster *miFirst = (redisMaster*)(listLength(g_pserver->masters) ? listNodeValue(listFirst(g_pserver->masters)) : NULL);

    /* If the instance is a replica we need a connected master
     * in order to fetch the currently selected DB. */
    if (miFirst && miFirst->master) {
        rsi->repl_stream_db = miFirst->master->db->id;
        return rsi;
    }

    /* If we have a cached master we can use it in order to populate the
     * replication selected DB info inside the RDB file: the replica can
     * increment the master_repl_offset only from data arriving from the
     * master, so if we are disconnected the offset in the cached master
     * is valid. */
    if (miFirst && miFirst->cached_master) {
        rsi->repl_stream_db = miFirst->cached_master->db->id;
        return rsi;
    }
    return NULL;
}
