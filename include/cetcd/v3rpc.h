#ifndef CETCD_V3RPC_H_
#define CETCD_V3RPC_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/base.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_v3rpc cetcd_v3rpc;

typedef struct {
    uint8_t *data;
    size_t   len;
} cetcd_rpc_bytes;

cetcd_v3rpc *cetcd_v3rpc_new(void);
void         cetcd_v3rpc_free(cetcd_v3rpc *rpc);

cetcd_rpc_bytes cetcd_v3rpc_dispatch(cetcd_v3rpc *rpc,
                                      const char *path,
                                      const uint8_t *req_data,
                                      size_t req_len);

void cetcd_rpc_bytes_free(cetcd_rpc_bytes *b);

#ifdef __cplusplus
}
#endif
#endif
