#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#  define _POSIX_C_SOURCE 200809L
#endif

#include "cetcd/base.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

static cetcd_log_level  g_level  = CETCD_LOG_INFO;
static cetcd_log_format g_format = CETCD_LOG_FORMAT_TEXT;
static FILE            *g_sink   = NULL;

static FILE *log_sink_(void) {
    return g_sink ? g_sink : stderr;
}

void cetcd_log_set_level(cetcd_log_level lvl) { g_level = lvl; }
cetcd_log_level cetcd_log_get_level(void)     { return g_level; }
void cetcd_log_set_format(cetcd_log_format f) { g_format = f; }
cetcd_log_format cetcd_log_get_format(void)   { return g_format; }
void cetcd_log_set_sink(FILE *fp)             { g_sink = fp; }
FILE *cetcd_log_get_sink(void)                { return log_sink_(); }

const char *cetcd_log_level_name(cetcd_log_level lvl) {
    switch (lvl) {
    case CETCD_LOG_TRACE: return "TRACE";
    case CETCD_LOG_DEBUG: return "DEBUG";
    case CETCD_LOG_INFO:  return "INFO";
    case CETCD_LOG_WARN:  return "WARN";
    case CETCD_LOG_ERROR: return "ERROR";
    case CETCD_LOG_FATAL: return "FATAL";
    case CETCD_LOG_OFF:   return "OFF";
    }
    return "?";
}

static void emit_text_(FILE *fp, cetcd_log_level lvl,
                        const char *file, int line, const char *func,
                        const char *fmt, va_list ap) {
    uint64_t now_ns = cetcd_clock_realtime_ns();
    time_t   sec    = (time_t)(now_ns / 1000000000ull);
    int      usec   = (int)((now_ns / 1000ull) % 1000000ull);
    struct tm tmv;
#if defined(_WIN32)
    gmtime_s(&tmv, &sec);
#else
    gmtime_r(&sec, &tmv);
#endif
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S", &tmv);

    const char *base = file;
    const char *slash = strrchr(file, '/');
    if (slash) base = slash + 1;

    fprintf(fp, "%s.%06dZ %-5s %s:%d %s() ",
            ts, usec, cetcd_log_level_name(lvl), base, line, func);
    vfprintf(fp, fmt, ap);
    fputc('\n', fp);
    fflush(fp);
}

static void json_escape_(FILE *fp, const char *s) {
    fputc('"', fp);
    for (const char *p = s; *p; ++p) {
        unsigned char c = (unsigned char)*p;
        switch (c) {
        case '"':  fputs("\\\"", fp); break;
        case '\\': fputs("\\\\", fp); break;
        case '\n': fputs("\\n",  fp); break;
        case '\r': fputs("\\r",  fp); break;
        case '\t': fputs("\\t",  fp); break;
        default:
            if (c < 0x20) fprintf(fp, "\\u%04x", c);
            else          fputc((int)c, fp);
        }
    }
    fputc('"', fp);
}

static void emit_json_(FILE *fp, cetcd_log_level lvl,
                        const char *file, int line, const char *func,
                        const char *fmt, va_list ap) {
    char msg[1024];
    int n = vsnprintf(msg, sizeof(msg), fmt, ap);
    if (n < 0) msg[0] = '\0';

    uint64_t ts = cetcd_clock_realtime_ns();
    fprintf(fp, "{\"ts_ns\":%llu,\"level\":\"%s\",\"file\":\"%s\",\"line\":%d,\"func\":\"%s\",\"msg\":",
            (unsigned long long)ts,
            cetcd_log_level_name(lvl),
            file, line, func);
    json_escape_(fp, msg);
    fputs("}\n", fp);
    fflush(fp);
}

void cetcd_log_vemit(cetcd_log_level lvl,
                     const char *file, int line, const char *func,
                     const char *fmt, va_list ap) {
    if (lvl < g_level) return;
    FILE *fp = log_sink_();
    if (g_format == CETCD_LOG_FORMAT_JSON) emit_json_(fp, lvl, file, line, func, fmt, ap);
    else                                    emit_text_(fp, lvl, file, line, func, fmt, ap);
}

void cetcd_log_emit(cetcd_log_level lvl,
                    const char *file, int line, const char *func,
                    const char *fmt, ...) {
    if (lvl < g_level) return;
    va_list ap;
    va_start(ap, fmt);
    cetcd_log_vemit(lvl, file, line, func, fmt, ap);
    va_end(ap);
}
