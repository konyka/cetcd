#ifndef CETCD_LOG_H_
#define CETCD_LOG_H_

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#if defined(__GNUC__) || defined(__clang__)
#  define CETCD_PRINTF_(fmt_idx, arg_idx) __attribute__((format(printf, fmt_idx, arg_idx)))
#else
#  define CETCD_PRINTF_(fmt_idx, arg_idx)
#endif

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

/* etcd aliases: warning→warn; dpanic/panic/fatal→error. */
int              cetcd_parse_log_level(const char *s, cetcd_log_level *out);
/* json, or text (etcd console = text). */
int              cetcd_parse_log_format(const char *s, cetcd_log_format *out);
/* zap or capnslog. Other types are INVAL. */
int              cetcd_parse_logger(const char *s);

/* Parse --log-outputs: stderr/stdout (/dev/std{err,out}), a file path
 * (append), or journal/syslog/systemd/journal. etcd `default` is INVAL
 * (zap does not support it). Mixed comma-lists fail-closed. *owned is
 * the FILE to fclose at shutdown (NULL for stdio). */
int              cetcd_log_open_outputs(const char *spec, FILE **owned);

/* Connect a SOCK_DGRAM client to a syslog/journal unix socket.
 * NULL path tries /run/systemd/journal/dev-log then /dev/log.
 * Windows is fail-closed (UNSUPPORT). */
int              cetcd_log_open_journal(const char *socket_path, FILE **owned);

/* etcd --log-rotation-config-json (lumberjack). maxsize 0 → 100 MiB.
 * compress true is UNSUPPORT (no gzip). */
typedef struct cetcd_log_rotation_cfg {
    uint32_t maxsize_mb;
    uint32_t maxage_days;
    uint32_t maxbackups;
    int      localtime;
    int      compress;
} cetcd_log_rotation_cfg;

void cetcd_log_rotation_cfg_default(cetcd_log_rotation_cfg *cfg);
int  cetcd_parse_log_rotation_json(const char *s, cetcd_log_rotation_cfg *cfg);
/* Unset → 0 (off). set uses enabled. */
int  cetcd_log_want_rotation(int set, int enabled);
/* Single file path only. stderr/stdout/journal/comma is INVAL. */
int  cetcd_log_outputs_single_file(const char *spec, char *out, size_t cap);
/* 1 if size >= maxsize MiB. maxsize 0 is 100. */
int  cetcd_log_should_rotate(uint64_t size_bytes, uint32_t maxsize_mb);
int  cetcd_log_rotation_backup_name(const char *path, uint64_t epoch_ns,
                                    int localtime, char *out, size_t cap);
/* Sink must already be the file. Rotation owns that FILE after this. */
int  cetcd_log_enable_rotation(const char *path, const cetcd_log_rotation_cfg *cfg);
int  cetcd_log_rotate_now(uint64_t now_ns);
void cetcd_log_rotation_close(void);

void cetcd_log_emit(cetcd_log_level lvl,
                    const char *file, int line, const char *func,
                    const char *fmt, ...) CETCD_PRINTF_(5, 6);

void cetcd_log_vemit(cetcd_log_level lvl,
                     const char *file, int line, const char *func,
                     const char *fmt, va_list ap) CETCD_PRINTF_(5, 0);

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
