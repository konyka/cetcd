#ifndef CETCD_BASE_H_
#define CETCD_BASE_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32)
#  define CETCD_API __declspec(dllexport)
#else
#  define CETCD_API __attribute__((visibility("default")))
#endif

#define CETCD_VERSION_MAJOR 0
#define CETCD_VERSION_MINOR 1
#define CETCD_VERSION_PATCH 0

typedef enum cetcd_status {
    CETCD_OK            =  0,
    CETCD_ERR_NOMEM     = -1,
    CETCD_ERR_INVAL     = -2,
    CETCD_ERR_RANGE     = -3,
    CETCD_ERR_NOTFOUND  = -4,
    CETCD_ERR_EXISTS    = -5,
    CETCD_ERR_IO        = -6,
    CETCD_ERR_CORRUPT   = -7,
    CETCD_ERR_INTERNAL  = -8,
    CETCD_ERR_OVERFLOW  = -9,
    CETCD_ERR_CANCELED  = -10,
    CETCD_ERR_TIMEDOUT  = -11,
    CETCD_ERR_UNSUPPORT = -12
} cetcd_status;

CETCD_API const char *cetcd_strerror(cetcd_status s);
CETCD_API const char *cetcd_version(void);

#include "cetcd/slice.h"
#include "cetcd/buf.h"
#include "cetcd/clock.h"
#include "cetcd/log.h"
#include "cetcd/arena.h"
#include "cetcd/slab.h"
#include "cetcd/hash.h"
#include "cetcd/hashmap.h"
#include "cetcd/treap.h"

#ifdef __cplusplus
}
#endif
#endif
