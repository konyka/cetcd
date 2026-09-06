#include "cetcd/peer.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

static void trim_(char *s) {
    if (!s) return;
    char *p = s;
    while (*p == ' ' || *p == '\t') p++;
    if (p != s) memmove(s, p, strlen(p) + 1);
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == ' ' || s[n - 1] == '\t'))
        s[--n] = '\0';
}

static int parse_one_(char *tok, cetcd_peer_info *pi, int *https) {
    char *eq = strchr(tok, '=');
    if (!eq || eq == tok || !eq[1]) return CETCD_ERR_INVAL;
    *eq = '\0';
    trim_(tok);
    char *addr_part = eq + 1;
    trim_(addr_part);
    if (!tok[0] || !addr_part[0]) return CETCD_ERR_INVAL;

    char *id_end = NULL;
    errno = 0;
    unsigned long long nid = strtoull(tok, &id_end, 10);
    if (errno == ERANGE || !id_end || id_end == tok || *id_end || nid < 1)
        return CETCD_ERR_INVAL;
    pi->id = (uint64_t)nid;
    pi->is_learner = 0;
    pi->addr[0] = '\0';
    pi->port = 2380;

    if (strncmp(addr_part, "https://", 8) == 0) {
        if (https) *https = 1;
        addr_part += 8;
    } else if (strncmp(addr_part, "http://", 7) == 0) {
        addr_part += 7;
    }
    if (!addr_part[0]) return CETCD_ERR_INVAL;

    char *colon = strrchr(addr_part, ':');
    if (colon) {
        *colon = '\0';
        if (!addr_part[0]) return CETCD_ERR_INVAL;
        char *end = NULL;
        errno = 0;
        long v = strtol(colon + 1, &end, 10);
        if (errno == ERANGE || !end || end == colon + 1 || *end ||
            v < 1 || v > 65535)
            return CETCD_ERR_RANGE;
        pi->port = (uint16_t)v;
    }
    strncpy(pi->addr, addr_part, sizeof(pi->addr) - 1);
    pi->addr[sizeof(pi->addr) - 1] = '\0';
    if (!pi->addr[0]) return CETCD_ERR_INVAL;
    return CETCD_OK;
}

int cetcd_parse_initial_cluster(const char *spec,
                                cetcd_peer_info *out, uint32_t cap,
                                uint32_t *n_out, int *https_out) {
    if (n_out) *n_out = 0;
    if (https_out) *https_out = 0;
    if (!spec || !spec[0] || !out || cap == 0 || !n_out)
        return CETCD_ERR_INVAL;

    char buf[2048];
    size_t slen = strlen(spec);
    if (slen >= sizeof(buf)) return CETCD_ERR_INVAL;
    memcpy(buf, spec, slen + 1);

    int https = 0;
    uint32_t n = 0;
    char *p = buf;
    while (p) {
        char *comma = strchr(p, ',');
        if (comma) *comma = '\0';
        trim_(p);
        if (!p[0]) return CETCD_ERR_INVAL;
        if (n >= cap) return CETCD_ERR_INVAL;
        cetcd_peer_info pi;
        memset(&pi, 0, sizeof(pi));
        int rc = parse_one_(p, &pi, &https);
        if (rc != CETCD_OK) return rc;
        for (uint32_t i = 0; i < n; i++) {
            if (out[i].id == pi.id) return CETCD_ERR_INVAL;
        }
        out[n++] = pi;
        p = comma ? comma + 1 : NULL;
    }
    if (n == 0) return CETCD_ERR_INVAL;
    *n_out = n;
    if (https_out) *https_out = https;
    return CETCD_OK;
}
