#ifndef CETCD_HTTP2_H_
#define CETCD_HTTP2_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_h2_session cetcd_h2_session;
typedef struct cetcd_h2_stream  cetcd_h2_stream;

typedef struct cetcd_h2_callbacks cetcd_h2_callbacks;

typedef void (*cetcd_h2_on_request_fn)(cetcd_h2_session *sess,
                                        int32_t stream_id,
                                        const char *method,
                                        const char *path,
                                        const char *content_type,
                                        void *udata);

typedef void (*cetcd_h2_on_data_fn)(cetcd_h2_session *sess,
                                     int32_t stream_id,
                                     const uint8_t *data,
                                     size_t len,
                                     bool end_stream,
                                     void *udata);

struct cetcd_h2_callbacks {
    cetcd_h2_on_request_fn on_request;
    cetcd_h2_on_data_fn    on_data;
    void                  *udata;
};

/* SETTINGS_MAX_CONCURRENT_STREAMS. 0 = omit (nghttp2 default). */
#define CETCD_H2_SETTINGS_MAX_CONCURRENT_STREAMS 3u
/* Hard cap on tracked streams (SETTINGS is clamped to this). */
#define CETCD_H2_MAX_STREAMS 64
void     cetcd_h2_set_max_concurrent_streams(uint32_t n);
uint32_t cetcd_h2_max_concurrent_streams(void);
/* 1 and fills id/val when n > 0. n above CETCD_H2_MAX_STREAMS is clamped. */
int cetcd_h2_fill_max_concurrent_setting(uint32_t n, uint32_t *id, uint32_t *val);

/* Per-stream header/body slot (session table + tests). */
typedef struct cetcd_h2_stream_slot {
    int32_t stream_id;
    char    method[16];
    char    path[256];
    char    content_type[64];
    char    authorization[2048];
    int     in_use;
    int     request_notified;
    uint8_t *resp_body;
    size_t  resp_body_len;
    size_t  resp_body_pos;
} cetcd_h2_stream_slot;

cetcd_h2_stream_slot *cetcd_h2_slot_begin(cetcd_h2_stream_slot *tab, size_t n,
                                          int32_t sid);
cetcd_h2_stream_slot *cetcd_h2_slot_get(cetcd_h2_stream_slot *tab, size_t n,
                                        int32_t sid);
void cetcd_h2_slot_clear(cetcd_h2_stream_slot *st);
const char *cetcd_h2_slot_authorization(const cetcd_h2_stream_slot *tab,
                                        size_t n, int32_t sid);

cetcd_h2_session *cetcd_h2_session_new(const cetcd_h2_callbacks *cbs);
cetcd_h2_session *cetcd_h2_session_new_client(const cetcd_h2_callbacks *cbs);
void              cetcd_h2_session_free(cetcd_h2_session *s);

/* Client: POST (or other method) with optional body. END_STREAM after DATA. */
int cetcd_h2_submit_request(cetcd_h2_session *s,
                            const char *method, const char *path,
                            const uint8_t *body, size_t body_len);

int cetcd_h2_feed(cetcd_h2_session *s, const uint8_t *data, size_t len);
int cetcd_h2_send_pending(cetcd_h2_session *s,
                           int (*write_fn)(const uint8_t *buf, size_t len, void *ctx),
                           void *ctx);

int cetcd_h2_submit_response(cetcd_h2_session *s, int32_t stream_id,
                               const char **headers, size_t header_count,
                               const uint8_t *body, size_t body_len,
                               bool end_stream);

/* Additional DATA on an open stream (Watch events). end_stream closes it. */
int cetcd_h2_submit_data(cetcd_h2_session *s, int32_t stream_id,
                         const uint8_t *body, size_t body_len, bool end_stream);

int cetcd_h2_submit_trailers(cetcd_h2_session *s, int32_t stream_id,
                               const char **trailers, size_t count);

void cetcd_h2_session_terminate(cetcd_h2_session *s, uint32_t error_code);

/* 1 if `data` is the HTTP/2 client preface, 0 if it cannot be, -1 if more bytes
 * are required. Used to demux gRPC from cetcdctl's length-prefixed frames. */
int cetcd_h2_detect(const uint8_t *data, size_t len);

/* Authorization of the last notified request, or "" if none. */
const char *cetcd_h2_req_authorization(const cetcd_h2_session *s);
/* Authorization of stream_id, or "" if none. */
const char *cetcd_h2_req_authorization_on(const cetcd_h2_session *s,
                                          int32_t stream_id);

/* gRPC framing helpers */
int  cetcd_grpc_encode(const uint8_t *msg, size_t msg_len,
                        bool compressed, uint8_t **out, size_t *out_len);

int  cetcd_grpc_decode(const uint8_t *frame, size_t frame_len,
                        bool *compressed, uint8_t **msg, size_t *msg_len);

#ifdef __cplusplus
}
#endif
#endif
