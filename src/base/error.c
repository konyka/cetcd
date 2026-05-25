#include "cetcd/base.h"

#define CETCD_VERSION_STR_(a, b, c) #a "." #b "." #c
#define CETCD_VERSION_STR(a, b, c)  CETCD_VERSION_STR_(a, b, c)

const char *cetcd_strerror(cetcd_status s) {
    switch (s) {
    case CETCD_OK:            return "ok";
    case CETCD_ERR_NOMEM:     return "out of memory";
    case CETCD_ERR_INVAL:     return "invalid argument";
    case CETCD_ERR_RANGE:     return "value out of range";
    case CETCD_ERR_NOTFOUND:  return "not found";
    case CETCD_ERR_EXISTS:    return "already exists";
    case CETCD_ERR_IO:        return "i/o error";
    case CETCD_ERR_CORRUPT:   return "data corruption";
    case CETCD_ERR_INTERNAL:  return "internal error";
    case CETCD_ERR_OVERFLOW:  return "overflow";
    case CETCD_ERR_CANCELED:  return "canceled";
    case CETCD_ERR_TIMEDOUT:  return "timed out";
    case CETCD_ERR_UNSUPPORT: return "unsupported";
    }
    return "unknown error";
}

const char *cetcd_version(void) {
    return CETCD_VERSION_STR(CETCD_VERSION_MAJOR,
                              CETCD_VERSION_MINOR,
                              CETCD_VERSION_PATCH);
}
