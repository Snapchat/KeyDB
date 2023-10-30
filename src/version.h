#define KEYDB_REAL_VERSION "6.3.4"
#define KEYDB_VERSION_NUM 0x00060304
extern const char *KEYDB_SET_VERSION;   // Unlike real version, this can be overriden by the config

enum VersionCompareResult
{
    EqualVersion,
    OlderVersion,
    NewerVersion,
    IncompatibleVersion,
};

struct SymVer
{
    long major;
    long minor;
    long build;
};

#ifdef __cplusplus
extern "C"
{
#endif

struct SymVer parseVersion(const char *version);
enum VersionCompareResult compareVersion(struct SymVer *pver);

#ifdef __cplusplus
}
#endif
