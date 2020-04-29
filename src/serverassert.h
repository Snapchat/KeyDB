#pragma once

void _serverAssertWithInfo(const struct client *c, class robj_roptr o, const char *estr, const char *file, int line);
extern "C" void _serverAssert(const char *estr, const char *file, int line);
extern "C" void _serverPanic(const char *file, int line, const char *msg, ...);

/* We can print the stacktrace, so our assert is defined this way: */
#define serverAssertWithInfo(_c,_o,_e) ((_e)?(void)0 : (_serverAssertWithInfo(_c,_o,#_e,__FILE__,__LINE__),_exit(1)))
#define serverAssert(_e) ((_e)?(void)0 : (_serverAssert(#_e,__FILE__,__LINE__),_exit(1)))
#ifdef _DEBUG
#define serverAssertDebug(_e) serverAssert(_e)
#else
#define serverAssertDebug(_e)
#endif
#define serverPanic(...) _serverPanic(__FILE__,__LINE__,__VA_ARGS__),_exit(1)