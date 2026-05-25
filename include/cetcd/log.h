#ifndef CETCD_LOG_H_
#define CETCD_LOG_H_

#include <stdarg.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum cetcd_log_level {
    CETCD_LOG_TRACE = 0,
    CETCD_LOG_DEBUG = 1,
    CETCD_LOG_INFO  = 2,
    CETCD_LOG_WARN  = 3,
    CETCD_LOG_ERROR = 4,
    CETCD_LOG_FATAL = 5,
    CETCD_LOG_OFF   = 99
} cetcd_log_level;

typedef enum cetcd_log_format {
    CETCD_LOG_FORMAT_TEXT = 0,
    CETCD_LOG_FORMAT_JSON = 1
} cetcd_log_format;

void             cetcd_log_set_level(cetcd_log_level lvl);
cetcd_log_level  cetcd_log_get_level(void);
void             cetcd_log_set_format(cetcd_log_format fmt);
cetcd_log_format cetcd_log_get_format(void);
void             cetcd_log_set_sink(FILE *fp);
FILE            *cetcd_log_get_sink(void);

void cetcd_log_emit(cetcd_log_level lvl,
                    const char *file, int line, const char *func,
                    const char *fmt, ...);

void cetcd_log_vemit(cetcd_log_level lvl,
                     const char *file, int line, const char *func,
                     const char *fmt, va_list ap);

#define CETCD_LOG(lvl, ...) \
    cetcd_log_emit((lvl), __FILE__, __LINE__, __func__, __VA_ARGS__)

#define CETCD_TRACE(...) CETCD_LOG(CETCD_LOG_TRACE, __VA_ARGS__)
#define CETCD_DEBUG(...) CETCD_LOG(CETCD_LOG_DEBUG, __VA_ARGS__)
#define CETCD_INFO(...)  CETCD_LOG(CETCD_LOG_INFO,  __VA_ARGS__)
#define CETCD_WARN(...)  CETCD_LOG(CETCD_LOG_WARN,  __VA_ARGS__)
#define CETCD_ERROR(...) CETCD_LOG(CETCD_LOG_ERROR, __VA_ARGS__)
#define CETCD_FATAL(...) CETCD_LOG(CETCD_LOG_FATAL, __VA_ARGS__)

const char *cetcd_log_level_name(cetcd_log_level lvl);

#ifdef __cplusplus
}
#endif
#endif
