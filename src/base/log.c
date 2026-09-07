#if !defined(_WIN32)
#  ifndef _DEFAULT_SOURCE
#    define _DEFAULT_SOURCE
#  endif
#endif

#include "cetcd/base.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#if defined(_WIN32)
#  include <direct.h>
#  include <io.h>
#  include <windows.h>
#else
#  include <dirent.h>
#  include <sys/socket.h>
#  include <sys/un.h>
#  include <unistd.h>
#endif

static cetcd_log_level  g_level  = CETCD_LOG_INFO;
static cetcd_log_format g_format = CETCD_LOG_FORMAT_TEXT;
static FILE            *g_sink   = NULL;

typedef struct log_rot_state_ {
    int                    on;
    cetcd_log_rotation_cfg cfg;
    char                   path[512];
    FILE                  *owned;
} log_rot_state_;

static log_rot_state_ g_rot;

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

static void log_maybe_rotate_(void);

void cetcd_log_vemit(cetcd_log_level lvl,
                     const char *file, int line, const char *func,
                     const char *fmt, va_list ap) {
    if (lvl < g_level) return;
    FILE *fp = log_sink_();
    if (g_format == CETCD_LOG_FORMAT_JSON) emit_json_(fp, lvl, file, line, func, fmt, ap);
    else                                    emit_text_(fp, lvl, file, line, func, fmt, ap);
    log_maybe_rotate_();
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

int cetcd_parse_log_level(const char *s, cetcd_log_level *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (strcmp(s, "trace") == 0) { *out = CETCD_LOG_TRACE; return CETCD_OK; }
    if (strcmp(s, "debug") == 0) { *out = CETCD_LOG_DEBUG; return CETCD_OK; }
    if (strcmp(s, "info") == 0) { *out = CETCD_LOG_INFO; return CETCD_OK; }
    if (strcmp(s, "warn") == 0 || strcmp(s, "warning") == 0) {
        *out = CETCD_LOG_WARN;
        return CETCD_OK;
    }
    if (strcmp(s, "error") == 0 || strcmp(s, "dpanic") == 0 ||
        strcmp(s, "panic") == 0 || strcmp(s, "fatal") == 0) {
        *out = CETCD_LOG_ERROR;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

int cetcd_parse_log_format(const char *s, cetcd_log_format *out) {
    if (!s || !s[0] || !out) return CETCD_ERR_INVAL;
    if (strcmp(s, "json") == 0) { *out = CETCD_LOG_FORMAT_JSON; return CETCD_OK; }
    if (strcmp(s, "text") == 0 || strcmp(s, "console") == 0) {
        *out = CETCD_LOG_FORMAT_TEXT;
        return CETCD_OK;
    }
    return CETCD_ERR_INVAL;
}

int cetcd_parse_logger(const char *s) {
    if (!s || !s[0]) return CETCD_ERR_INVAL;
    if (strcmp(s, "zap") == 0 || strcmp(s, "capnslog") == 0) return CETCD_OK;
    return CETCD_ERR_INVAL;
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
        if (strcmp(tok, "default") == 0) return CETCD_ERR_INVAL;
        if (strcmp(tok, "journal") == 0 || strcmp(tok, "syslog") == 0 ||
            strcmp(tok, "systemd/journal") == 0) {
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

void cetcd_log_rotation_cfg_default(cetcd_log_rotation_cfg *cfg) {
    if (!cfg) return;
    cfg->maxsize_mb = 100;
    cfg->maxage_days = 0;
    cfg->maxbackups = 0;
    cfg->localtime = 0;
    cfg->compress = 0;
}

int cetcd_log_want_rotation(int set, int enabled) {
    return set ? (enabled != 0) : 0;
}

static const char *skip_ws_(const char *p) {
    while (p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

static int parse_json_uint_(const char **pp, uint32_t *out) {
    const char *p = skip_ws_(*pp);
    if (!p || *p < '0' || *p > '9') return -1;
    unsigned long v = 0;
    while (*p >= '0' && *p <= '9') {
        v = v * 10ul + (unsigned long)(*p - '0');
        if (v > 0xFFFFFFFFul) return -1;
        p++;
    }
    *out = (uint32_t)v;
    *pp = p;
    return 0;
}

static int parse_json_bool_(const char **pp, int *out) {
    const char *p = skip_ws_(*pp);
    if (strncmp(p, "true", 4) == 0 && !isalnum((unsigned char)p[4])) {
        *out = 1;
        *pp = p + 4;
        return 0;
    }
    if (strncmp(p, "false", 5) == 0 && !isalnum((unsigned char)p[5])) {
        *out = 0;
        *pp = p + 5;
        return 0;
    }
    return -1;
}

int cetcd_parse_log_rotation_json(const char *s, cetcd_log_rotation_cfg *cfg) {
    if (!s || !s[0] || !cfg) return CETCD_ERR_INVAL;
    cetcd_log_rotation_cfg_default(cfg);
    const char *p = skip_ws_(s);
    if (*p != '{') return CETCD_ERR_INVAL;
    p++;
    p = skip_ws_(p);
    if (*p == '}') {
        p = skip_ws_(p + 1);
        return *p ? CETCD_ERR_INVAL : CETCD_OK;
    }
    for (;;) {
        p = skip_ws_(p);
        if (*p != '"') return CETCD_ERR_INVAL;
        p++;
        char key[32];
        size_t kn = 0;
        while (*p && *p != '"' && kn + 1 < sizeof(key)) key[kn++] = *p++;
        if (*p != '"') return CETCD_ERR_INVAL;
        key[kn] = '\0';
        p++;
        p = skip_ws_(p);
        if (*p != ':') return CETCD_ERR_INVAL;
        p++;
        if (strcmp(key, "maxsize") == 0) {
            if (parse_json_uint_(&p, &cfg->maxsize_mb) != 0) return CETCD_ERR_INVAL;
        } else if (strcmp(key, "maxage") == 0) {
            if (parse_json_uint_(&p, &cfg->maxage_days) != 0) return CETCD_ERR_INVAL;
        } else if (strcmp(key, "maxbackups") == 0) {
            if (parse_json_uint_(&p, &cfg->maxbackups) != 0) return CETCD_ERR_INVAL;
        } else if (strcmp(key, "localtime") == 0) {
            if (parse_json_bool_(&p, &cfg->localtime) != 0) return CETCD_ERR_INVAL;
        } else if (strcmp(key, "compress") == 0) {
            int c = 0;
            if (parse_json_bool_(&p, &c) != 0) return CETCD_ERR_INVAL;
            if (c) return CETCD_ERR_UNSUPPORT;
            cfg->compress = 0;
        } else {
            return CETCD_ERR_INVAL;
        }
        p = skip_ws_(p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == '}') {
            p = skip_ws_(p + 1);
            return *p ? CETCD_ERR_INVAL : CETCD_OK;
        }
        return CETCD_ERR_INVAL;
    }
}

int cetcd_log_outputs_single_file(const char *spec, char *out, size_t cap) {
    if (!spec || !spec[0] || !out || cap == 0) return CETCD_ERR_INVAL;
    if (strchr(spec, ',')) return CETCD_ERR_INVAL;
    const char *tok = spec;
    while (*tok == ' ' || *tok == '\t') tok++;
    size_t n = strlen(tok);
    while (n > 0 && (tok[n - 1] == ' ' || tok[n - 1] == '\t')) n--;
    if (n == 0 || n >= cap) return CETCD_ERR_INVAL;
    memcpy(out, tok, n);
    out[n] = '\0';
    if (strcmp(out, "stderr") == 0 || strcmp(out, "/dev/stderr") == 0)
        return CETCD_ERR_INVAL;
    if (strcmp(out, "stdout") == 0 || strcmp(out, "/dev/stdout") == 0)
        return CETCD_ERR_INVAL;
    if (strcmp(out, "journal") == 0 || strcmp(out, "syslog") == 0)
        return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_log_should_rotate(uint64_t size_bytes, uint32_t maxsize_mb) {
    uint64_t mb = maxsize_mb ? maxsize_mb : 100u;
    return size_bytes >= mb * 1048576ull;
}

int cetcd_log_rotation_backup_name(const char *path, uint64_t epoch_ns,
                                   int localtime, char *out, size_t cap) {
    if (!path || !path[0] || !out || cap < 8) return CETCD_ERR_INVAL;
    const char *slash = strrchr(path, '/');
#if defined(_WIN32)
    const char *bsl = strrchr(path, '\\');
    if (bsl && (!slash || bsl > slash)) slash = bsl;
#endif
    const char *base = slash ? slash + 1 : path;
    size_t dir_len = (size_t)(base - path);
    const char *dot = strrchr(base, '.');
    size_t stem_len = dot ? (size_t)(dot - base) : strlen(base);
    const char *ext = dot ? dot : "";
    if (stem_len == 0) return CETCD_ERR_INVAL;

    time_t sec = (time_t)(epoch_ns / 1000000000ull);
    int msec = (int)((epoch_ns / 1000000ull) % 1000ull);
    struct tm tmv;
#if defined(_WIN32)
    if (localtime) localtime_s(&tmv, &sec);
    else gmtime_s(&tmv, &sec);
#else
    if (localtime) localtime_r(&sec, &tmv);
    else gmtime_r(&sec, &tmv);
#endif
    int n = snprintf(out, cap, "%.*s%.*s-%04d-%02d-%02dT%02d-%02d-%02d.%03d%s",
                     (int)dir_len, path, (int)stem_len, base,
                     tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                     tmv.tm_hour, tmv.tm_min, tmv.tm_sec, msec, ext);
    if (n <= 0 || (size_t)n >= cap) return CETCD_ERR_OVERFLOW;
    return CETCD_OK;
}

typedef struct rot_backup_ {
    char   path[512];
    time_t mtime;
} rot_backup_;

static int rot_backup_cmp_(const void *a, const void *b) {
    const rot_backup_ *x = (const rot_backup_ *)a;
    const rot_backup_ *y = (const rot_backup_ *)b;
    if (x->mtime > y->mtime) return -1;
    if (x->mtime < y->mtime) return 1;
    return 0;
}

static int rot_is_backup_(const char *name, const char *stem, const char *ext) {
    size_t sl = strlen(stem);
    size_t el = strlen(ext);
    size_t nl = strlen(name);
    if (nl < sl + 1 + el) return 0;
    if (memcmp(name, stem, sl) != 0 || name[sl] != '-') return 0;
    if (el && memcmp(name + nl - el, ext, el) != 0) return 0;
    return 1;
}

static void rot_prune_(const char *path, const cetcd_log_rotation_cfg *cfg,
                       time_t now) {
    if (!path || !cfg) return;
    if (cfg->maxbackups == 0 && cfg->maxage_days == 0) return;

    const char *slash = strrchr(path, '/');
#if defined(_WIN32)
    const char *bsl = strrchr(path, '\\');
    if (bsl && (!slash || bsl > slash)) slash = bsl;
#endif
    const char *base = slash ? slash + 1 : path;
    char dir[512];
    if (slash) {
        size_t dl = (size_t)(slash - path);
        if (dl == 0) dl = 1;
        if (dl >= sizeof(dir)) return;
        memcpy(dir, path, dl);
        dir[dl] = '\0';
    } else {
        memcpy(dir, ".", 2);
    }
    const char *dot = strrchr(base, '.');
    char stem[256];
    size_t sl = dot ? (size_t)(dot - base) : strlen(base);
    if (sl == 0 || sl >= sizeof(stem)) return;
    memcpy(stem, base, sl);
    stem[sl] = '\0';
    const char *ext = dot ? dot : "";

    rot_backup_ ents[256];
    int n = 0;
#if defined(_WIN32)
    char pat[560];
    snprintf(pat, sizeof(pat), "%s\\%s-*%s", dir, stem, ext);
    WIN32_FIND_DATAA fd;
    HANDLE h = FindFirstFileA(pat, &fd);
    if (h == INVALID_HANDLE_VALUE) return;
    do {
        if (n >= 256) break;
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) continue;
        if (!rot_is_backup_(fd.cFileName, stem, ext)) continue;
        int wn = snprintf(ents[n].path, sizeof(ents[n].path), "%s\\%s",
                          dir, fd.cFileName);
        if (wn <= 0 || (size_t)wn >= sizeof(ents[n].path)) continue;
        struct stat st;
        if (stat(ents[n].path, &st) != 0) continue;
        ents[n].mtime = st.st_mtime;
        n++;
    } while (FindNextFileA(h, &fd));
    FindClose(h);
#else
    DIR *d = opendir(dir);
    if (!d) return;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (n >= 256) break;
        if (!rot_is_backup_(de->d_name, stem, ext)) continue;
        int wn = snprintf(ents[n].path, sizeof(ents[n].path), "%s/%s",
                          dir, de->d_name);
        if (wn <= 0 || (size_t)wn >= sizeof(ents[n].path)) continue;
        struct stat st;
        if (stat(ents[n].path, &st) != 0) continue;
        ents[n].mtime = st.st_mtime;
        n++;
    }
    closedir(d);
#endif
    if (n <= 0) return;
    qsort(ents, (size_t)n, sizeof(ents[0]), rot_backup_cmp_);
    time_t age = (cfg->maxage_days > 0)
                     ? (time_t)cfg->maxage_days * (time_t)86400
                     : 0;
    for (int i = 0; i < n; i++) {
        int drop = 0;
        if (cfg->maxbackups > 0 && (uint32_t)i >= cfg->maxbackups) drop = 1;
        if (age && now >= ents[i].mtime && (now - ents[i].mtime) > age) drop = 1;
        if (drop) remove(ents[i].path);
    }
}

int cetcd_log_enable_rotation(const char *path, const cetcd_log_rotation_cfg *cfg) {
    if (!path || !path[0] || !cfg) return CETCD_ERR_INVAL;
    if (cfg->compress) return CETCD_ERR_UNSUPPORT;
    FILE *fp = log_sink_();
    if (!fp || fp == stderr || fp == stdout) return CETCD_ERR_INVAL;
    if (strlen(path) >= sizeof(g_rot.path)) return CETCD_ERR_OVERFLOW;
    memset(&g_rot, 0, sizeof(g_rot));
    memcpy(&g_rot.cfg, cfg, sizeof(*cfg));
    if (g_rot.cfg.maxsize_mb == 0) g_rot.cfg.maxsize_mb = 100;
    strncpy(g_rot.path, path, sizeof(g_rot.path) - 1);
    g_rot.owned = fp;
    g_rot.on = 1;
    return CETCD_OK;
}

void cetcd_log_rotation_close(void) {
    if (g_rot.owned) {
        fclose(g_rot.owned);
        if (g_sink == g_rot.owned) g_sink = NULL;
        g_rot.owned = NULL;
    }
    g_rot.on = 0;
}

int cetcd_log_rotate_now(uint64_t now_ns) {
    if (!g_rot.on || !g_rot.path[0] || !g_rot.owned) return CETCD_ERR_INVAL;
    char bak[640];
    int nrc = cetcd_log_rotation_backup_name(g_rot.path, now_ns, g_rot.cfg.localtime,
                                             bak, sizeof(bak));
    if (nrc != CETCD_OK) return nrc;
    FILE *old = g_rot.owned;
    fflush(old);
    fclose(old);
    g_rot.owned = NULL;
    if (g_sink == old) g_sink = NULL;
#if defined(_WIN32)
    remove(bak);
#endif
    if (rename(g_rot.path, bak) != 0) {
        FILE *again = fopen(g_rot.path, "a");
        if (again) {
            g_rot.owned = again;
            cetcd_log_set_sink(again);
        }
        return CETCD_ERR_IO;
    }
    FILE *fp = fopen(g_rot.path, "a");
    if (!fp) return CETCD_ERR_IO;
    g_rot.owned = fp;
    cetcd_log_set_sink(fp);
    rot_prune_(g_rot.path, &g_rot.cfg, (time_t)(now_ns / 1000000000ull));
    return CETCD_OK;
}

static void log_maybe_rotate_(void) {
    if (!g_rot.on || !g_rot.owned) return;
    long sz = ftell(g_rot.owned);
    if (sz < 0) return;
    if (!cetcd_log_should_rotate((uint64_t)sz, g_rot.cfg.maxsize_mb)) return;
    (void)cetcd_log_rotate_now(cetcd_clock_realtime_ns());
}
