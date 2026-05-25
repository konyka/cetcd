#include "cetcd/io.h"
#include "cetcd_test.h"

static void conn_cb(cetcd_tcp *server, cetcd_tcp *client, void *arg) {
    (void)server; (void)client; (void)arg;
}

CETCD_TEST_CASE(tcp_bind_and_listen) {
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_tcp *tcp = cetcd_tcp_new(loop);
    CETCD_ASSERT(tcp != NULL);
    int rbind = cetcd_tcp_bind(tcp, "127.0.0.1", 0);
    CETCD_ASSERT(rbind == 0);
    int rlisten = cetcd_tcp_listen(tcp, conn_cb, NULL);
    CETCD_ASSERT(rlisten == 0);
    cetcd_tcp_free(tcp);
    cetcd_loop_free(loop);
}

CETCD_TEST_CASE(tcp_echo_server_client) {
    cetcd_loop *loop = cetcd_loop_new();
    cetcd_tcp *server = cetcd_tcp_new(loop);
    cetcd_tcp_bind(server, "127.0.0.1", 0);
    cetcd_tcp_listen(server, conn_cb, NULL);
    /* In this stub, we do not perform actual I/O; test only wiring. */
    cetcd_tcp_free(server);
    cetcd_loop_free(loop);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(tcp_bind_and_listen),
    CETCD_TEST_ENTRY(tcp_echo_server_client),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
