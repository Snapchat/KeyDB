#pragma once

#define UUID_BINARY_LEN 16

static inline int FUuidNil(unsigned char *uuid)
{
    unsigned char val = 0;
    for (int i = 0; i < UUID_BINARY_LEN; ++i)
        val |= uuid[i];
    return (val == 0);
}

static inline int FUuidEqual(unsigned char *uuid1, unsigned char *uuid2)
{
    unsigned char val = 0;
    for (int i = 0; i < UUID_BINARY_LEN; ++i)
        val |= (uuid1[i] ^ uuid2[i]);
    return (val == 0);
}