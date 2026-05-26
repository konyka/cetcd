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

cetcd_h2_session *cetcd_h2_session_new(const cetcd_h2_callbacks *cbs);
void              cetcd_h2_session_free(cetcd_h2_session *s);

int cetcd_h2_feed(cetcd_h2_session *s, const uint8_t *data, size_t len);
int cetcd_h2_send_pending(cetcd_h2_session *s,
                           int (*write_fn)(const uint8_t *buf, size_t len, void *ctx),
                           void *ctx);

int cetcd_h2_submit_response(cetcd_h2_session *s, int32_t stream_id,
                               const char **headers, size_t header_count,
                               const uint8_t *body, size_t body_len,
                               bool end_stream);

int cetcd_h2_submit_trailers(cetcd_h2_session *s, int32_t stream_id,
                               const char **trailers, size_t count);

void cetcd_h2_session_terminate(cetcd_h2_session *s, uint32_t error_code);

/* gRPC framing helpers */
int  cetcd_grpc_encode(const uint8_t *msg, size_t msg_len,
                        bool compressed, uint8_t **out, size_t *out_len);

int  cetcd_grpc_decode(const uint8_t *frame, size_t frame_len,
                        bool *compressed, uint8_t **msg, size_t *msg_len);

#ifdef __cplusplus
}
#endif
#endif
