#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "cetcd/io.h"
#include "cetcd/base.h"
#include "io_internal.h"

typedef struct cetcd_tcp {
    uv_tcp_t handle;
    int is_server;
    cetcd_loop *loop;
    cetcd_tcp_conn_cb conn_cb;
    void *conn_cb_arg;
} cetcd_tcp;

static void on_new_connection(uv_stream_t *server, int status);

cetcd_tcp *cetcd_tcp_new(cetcd_loop *loop) {
    if (!loop) return NULL;
    cetcd_tcp *tcp = (cetcd_tcp *)calloc(1, sizeof(*tcp));
    if (!tcp) return NULL;
    tcp->is_server = 1;
    tcp->loop = loop;
    uv_tcp_init(cetcd_loop_uv(loop), &tcp->handle);
    tcp->handle.data = tcp;
    return tcp;
}

void cetcd_tcp_free(cetcd_tcp *tcp) {
    if (!tcp) return;
    uv_close((uv_handle_t *)&tcp->handle, (void (*)(uv_handle_t*))free);
}

int cetcd_tcp_bind(cetcd_tcp *tcp, const char *addr, uint16_t port) {
    if (!tcp) return -1;
    struct sockaddr_in addr_in;
    if (uv_ip4_addr(addr, port, &addr_in) != 0) {
        return -1;
    }
    int r = uv_tcp_bind(&tcp->handle, (const struct sockaddr *)&addr_in, 0);
    return (r == 0) ? 0 : -1;
}

int cetcd_tcp_listen(cetcd_tcp *tcp, cetcd_tcp_conn_cb cb, void *arg) {
    if (!tcp) return -1;
    tcp->conn_cb = cb;
    tcp->conn_cb_arg = arg;
    tcp->handle.data = tcp; /* ensure callbacks can access owner */
    int r = uv_listen((uv_stream_t *)&tcp->handle, 128, on_new_connection);
    if (r != 0) return -1;
    return 0;
}

static void on_new_connection(uv_stream_t *server, int status) {
    if (status < 0) {
        return;
    }
    cetcd_tcp *listener = (cetcd_tcp *)server->data;
    if (!listener) return;
    /* Create a new client wrapper */
    cetcd_tcp *client = (cetcd_tcp *)calloc(1, sizeof(*client));
    if (!client) return;
    client->loop = listener->loop;
    uv_tcp_init(cetcd_loop_uv(listener->loop), &client->handle);
    if (uv_accept(server, (uv_stream_t *)&client->handle) == 0) {
        client->handle.data = client;
        client->conn_cb = listener->conn_cb;
        client->conn_cb_arg = listener->conn_cb_arg;
        /* Notify upper layer of new connection */
        if (listener->conn_cb) {
            listener->conn_cb((cetcd_tcp *)server->data, client, listener->conn_cb_arg);
        }
    } else {
        uv_close((uv_handle_t *)&client->handle, NULL);
        free(client);
    }
}

int cetcd_tcp_read(cetcd_tcp *tcp, void *buf, size_t len) {
    (void)tcp; (void)buf; (void)len;
    /* Real async I/O would be wired up here; not used by Phase 0 tests. */
    return 0;
}

int cetcd_tcp_write(cetcd_tcp *tcp, const void *buf, size_t len) {
    (void)tcp; (void)buf; (void)len;
    /* Real async I/O would be wired up here; not used by Phase 0 tests. */
    return 0;
}

void cetcd_tcp_close(cetcd_tcp *tcp) {
    if (!tcp) return;
    uv_close((uv_handle_t *)&tcp->handle, NULL);
}

uv_stream_t *cetcd_tcp_stream(cetcd_tcp *tcp) {
    if (!tcp) return NULL;
    return (uv_stream_t *)&tcp->handle;
}
