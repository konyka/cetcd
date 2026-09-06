#if !defined(_WIN32)
#  ifndef _DEFAULT_SOURCE
#    define _DEFAULT_SOURCE
#  endif
#endif

#include "cetcd/base.h"

#include <stdio.h>
#include <string.h>
#include <time.h>
#if !defined(_WIN32)
#  include <sys/socket.h>
#  include <sys/un.h>
#  include <unistd.h>
#endif

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
                        const char *fmt, va_list ap) CETCD_PRINTF_(6, 0);

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
                        const char *fmt, va_list ap) CETCD_PRINTF_(6, 0);

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

static int same_stdio_(const char *tok, FILE **sink) {
    if (strcmp(tok, "stderr") == 0 || strcmp(tok, "/dev/stderr") == 0) {
        *sink = stderr;
        return 1;
    }
    if (strcmp(tok, "stdout") == 0 || strcmp(tok, "/dev/stdout") == 0) {
        *sink = stdout;
        return 1;
    }
    return 0;
}

int cetcd_log_open_outputs(const char *spec, FILE **owned) {
    if (owned) *owned = NULL;
    if (!spec || !spec[0]) return CETCD_ERR_INVAL;
    char buf[512];
    if (strlen(spec) >= sizeof(buf)) return CETCD_ERR_OVERFLOW;
    memcpy(buf, spec, strlen(spec) + 1);

    FILE *chosen = NULL;
    int saw_file = 0;
    int ntok = 0;
    char *p = buf;
    while (*p) {
        char *comma = strchr(p, ',');
        if (comma) *comma = '\0';
        char *tok = p;
        while (*tok == ' ' || *tok == '\t') tok++;
        size_t tl = strlen(tok);
        while (tl > 0 && (tok[tl - 1] == ' ' || tok[tl - 1] == '\t'))
            tok[--tl] = '\0';
        if (tl == 0) return CETCD_ERR_INVAL;
        if (strcmp(tok, "journal") == 0 || strcmp(tok, "syslog") == 0) {
            if (chosen || saw_file) return CETCD_ERR_INVAL;
            FILE *j = NULL;
            int jc = cetcd_log_open_journal(NULL, &j);
            if (jc != 0) return jc;
            chosen = j;
            saw_file = 1; /* owned sink */
            ntok++;
            if (!comma) break;
            p = comma + 1;
            if (*p == '\0') return CETCD_ERR_INVAL;
            continue;
        }
        FILE *stdio = NULL;
        if (same_stdio_(tok, &stdio)) {
            if (saw_file) return CETCD_ERR_INVAL;
            if (chosen && chosen != stdio) return CETCD_ERR_INVAL;
            chosen = stdio;
        } else {
            if (chosen && !saw_file) return CETCD_ERR_INVAL;
            if (saw_file) return CETCD_ERR_INVAL;
            FILE *fp = fopen(tok, "a");
            if (!fp) return CETCD_ERR_IO;
            chosen = fp;
            saw_file = 1;
        }
        ntok++;
        if (!comma) break;
        p = comma + 1;
        if (*p == '\0') return CETCD_ERR_INVAL;
    }
    if (ntok == 0 || !chosen) return CETCD_ERR_INVAL;
    cetcd_log_set_sink(chosen);
    if (owned && saw_file) *owned = chosen;
    return 0;
}

int cetcd_log_open_journal(const char *socket_path, FILE **owned) {
    if (owned) *owned = NULL;
#if defined(_WIN32)
    (void)socket_path;
    return CETCD_ERR_UNSUPPORT;
#else
    const char *cands[3];
    int nc = 0;
    if (socket_path && socket_path[0]) {
        cands[nc++] = socket_path;
    } else {
        cands[nc++] = "/run/systemd/journal/dev-log";
        cands[nc++] = "/dev/log";
    }
    int fd = -1;
    for (int i = 0; i < nc; i++) {
        fd = socket(AF_UNIX, SOCK_DGRAM, 0);
        if (fd < 0) return CETCD_ERR_IO;
        struct sockaddr_un un;
        memset(&un, 0, sizeof(un));
        un.sun_family = AF_UNIX;
        size_t pl = strlen(cands[i]);
        if (pl == 0 || pl >= sizeof(un.sun_path)) {
            close(fd);
            fd = -1;
            continue;
        }
        memcpy(un.sun_path, cands[i], pl + 1);
        if (connect(fd, (struct sockaddr *)&un, sizeof(un)) == 0) break;
        close(fd);
        fd = -1;
    }
    if (fd < 0) return CETCD_ERR_IO;
    FILE *fp = fdopen(fd, "w");
    if (!fp) {
        close(fd);
        return CETCD_ERR_IO;
    }
    setvbuf(fp, NULL, _IOLBF, 0);
    cetcd_log_set_sink(fp);
    if (owned) *owned = fp;
    return 0;
#endif
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
