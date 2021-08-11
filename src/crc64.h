#ifndef CRC64_H
#define CRC64_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void crc64_init(void);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);

#ifdef REDIS_TEST
int crc64Test(int argc, char *argv[], int accurate);
#endif

#ifdef __cplusplus
}
#endif

#endif
