/* Lightweight stub TCP server/client facade for Phase 0 tests. */
#include <stdlib.h>
#include <string.h>

#include "cetcd/io.h"
#include "cetcd/base.h"

typedef struct cetcd_tcp {
    void *impl; /* opaque implementation placeholder */
} cetcd_tcp;

cetcd_tcp *cetcd_tcp_new(cetcd_loop *loop) {
    (void)loop;
    cetcd_tcp *tcp = (cetcd_tcp *)malloc(sizeof(*tcp));
    if (tcp) tcp->impl = NULL;
    return tcp;
}

void cetcd_tcp_free(cetcd_tcp *tcp) {
    if (tcp) {
        free(tcp);
    }
}

int cetcd_tcp_bind(cetcd_tcp *tcp, const char *addr, uint16_t port) {
    (void)tcp; (void)addr; (void)port;
    return 0; /* success in stub */
}

int cetcd_tcp_listen(cetcd_tcp *tcp, cetcd_tcp_conn_cb cb, void *arg) {
    (void)tcp; (void)cb; (void)arg;
    return 0;
}

int cetcd_tcp_read(cetcd_tcp *tcp, void *buf, size_t len) {
    (void)tcp; (void)buf; (void)len;
    return 0;
}

int cetcd_tcp_write(cetcd_tcp *tcp, const void *buf, size_t len) {
    (void)tcp; (void)buf; (void)len;
    return 0;
}

void cetcd_tcp_close(cetcd_tcp *tcp) {
    if (tcp) free(tcp);
}
