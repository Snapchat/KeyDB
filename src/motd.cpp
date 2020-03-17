#include "sds.h"
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
#include <sys/stat.h>
#include "motd.h"

/*------------------------------------------------------------------------------
 * Message of the day
 *--------------------------------------------------------------------------- */
#ifdef MOTD
#include <curl/curl.h> 

static const char *szMotdCachePath()
{
    static sds sdsMotdCachePath = NULL;
    if (sdsMotdCachePath != NULL)
        return sdsMotdCachePath;

    struct passwd *pw = getpwuid(getuid());
    if (pw == NULL)
        return "";
    const char *homedir = pw->pw_dir;
    sdsMotdCachePath = sdsnew(homedir);
    sdsMotdCachePath = sdscat(sdsMotdCachePath, motd_cache_file);
    return sdsMotdCachePath;
}
static size_t motd_write_callback(void *ptr, size_t size, size_t nmemb, sds *str)
{
    *str = sdscatlen(*str, ptr, size*nmemb);
    return (size*nmemb);
}

static char *fetchMOTDFromCache()
{
    struct stat attrib;
    if (stat(szMotdCachePath(), &attrib) != 0)
        return NULL;
    time_t t = attrib.st_mtim.tv_sec;
    time_t now = time(NULL);
    if ((now - t) < 14400)
    {
        // If our cache was updated no more than 4 hours ago use it instead of fetching the MOTD
        FILE *pf = fopen(szMotdCachePath(), "rb");
        if (pf == NULL)
            return NULL;
        fseek(pf, 0L, SEEK_END);
        long cb = ftell(pf);
        fseek(pf, 0L, SEEK_SET);    // rewind
        sds str = sdsnewlen(NULL, cb);
        size_t cbRead = fread(str, 1, cb, pf);
        fclose(pf);
        if ((long)cbRead != cb)
        {
            sdsfree(str);
            return NULL;
        }
        return str;
    }
    return NULL;
}

static void setMOTDCache(const char *sz)
{
    FILE *pf = fopen(szMotdCachePath(), "wb");
    if (pf == NULL)
        return;
    size_t celem = fwrite(sz, strlen(sz), 1, pf);
    (void)celem;    // best effort
    fclose(pf);
}

extern "C" char *fetchMOTD(int cache)
{
    sds str;
    CURL *curl;
    CURLcode res;

    /* First try and get the string from the cache */
    if (cache) {
        str = fetchMOTDFromCache();
        if (str != NULL)
            return str;
    }

    str = sdsnew("");
    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, motd_url);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L); // follow redirects
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2); // take no more than two seconds
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, motd_write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &str);

        /* Perform the request, res will get the return code */ 
        res = curl_easy_perform(curl);
        /* Check for errors */ 
        if(res != CURLE_OK)
        {
            sdsfree(str);
            str = NULL;
        }
        else
        {
            long response_code;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
            if ((response_code / 100) != 2)
            {
                // An error code not in the 200s implies an error
                sdsfree(str);
                str = NULL;
            }
        }

        /* always cleanup */ 
        curl_easy_cleanup(curl);

        if (str != NULL && cache)
            setMOTDCache(str);
    }
    return str;
}

#else

extern "C" char *fetchMOTD(int /* cache */)
{
    return NULL;
}

#endif