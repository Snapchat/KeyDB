#pragma once

extern const char *motd_url;

#ifdef __cplusplus
extern "C" {
#endif
    
char *fetchMOTD(int fCache);

#ifdef __cplusplus
}
#endif