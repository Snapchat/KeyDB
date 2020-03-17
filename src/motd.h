#pragma once

extern const char *motd_url;
extern const char *motd_cache_file;

#ifdef __cplusplus
extern "C" {
#endif
    
char *fetchMOTD(int fCache);

#ifdef __cplusplus
}
#endif