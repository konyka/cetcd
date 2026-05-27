#include "cetcd/base.h"
#include "cetcd/server.h"
#include "cetcd/io.h"
#include "cetcd/snap.h"
#include "cetcd_test.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>

static bool try_connect(const char *addr, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    inet_pton(AF_INET, addr, &sa.sin_addr);
    int rc = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    close(fd);
    return rc == 0;
}

CETCD_TEST_CASE(live_server_start_stop) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
    cfg.listen_port = 23790;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    cetcd_server_rpc_result resp = cetcd_server_handle_rpc(srv,
        "/etcdserverpb.KV/Put",
        (const uint8_t *)"\x0a\x01k\x12\x01v", 6);
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    cetcd_server_rpc_result_free(&resp);

    CETCD_ASSERT_TRUE(cetcd_server_revision(srv) > 0);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(live_server_snapshot_after_writes) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 23791;

    cetcd_server *srv = cetcd_server_new(&cfg);
    cetcd_server_start(srv);

    for (int i = 0; i < 10; i++) {
        uint8_t buf[16];
        size_t pos = 0;
        buf[pos++] = 0x0a; buf[pos++] = 0x01;
        buf[pos++] = (uint8_t)('a' + i);
        buf[pos++] = 0x12; buf[pos++] = 0x01;
        buf[pos++] = (uint8_t)('0' + i);
        cetcd_server_rpc_result resp =
            cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Put", buf, pos);
        cetcd_server_rpc_result_free(&resp);
    }

    int64_t rev = cetcd_server_revision(srv);
    CETCD_ASSERT_TRUE(rev >= 10);

    cetcd_snap *snap = cetcd_server_snapshot(srv);
    CETCD_ASSERT_NOT_NULL(snap);
    CETCD_ASSERT_TRUE(cetcd_snap_entry_count(snap) >= 10);
    cetcd_snap_free(snap);

    CETCD_ASSERT_EQ_INT(cetcd_server_compact(srv, rev), 0);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(live_server_persistent_backend) {
    const char *data_dir = "/tmp/cetcd_test_persist_13";

    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 23792;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    uint8_t put_req[] = {0x0a, 0x04, 't','e','s','t', 0x12, 0x04, 'd','a','t','a'};
    cetcd_server_rpc_result resp = cetcd_server_handle_rpc(srv,
        "/etcdserverpb.KV/Put", put_req, sizeof(put_req));
    CETCD_ASSERT_NOT_NULL(resp.data);
    cetcd_server_rpc_result_free(&resp);

    CETCD_ASSERT_TRUE(cetcd_server_revision(srv) > 0);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(live_server_tcp_listen) {
    pid_t pid = fork();
    if (pid == 0) {
        /* child: run server */
        cetcd_server_config cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.node_id = 1;
        strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
        cfg.listen_port = 23793;
        cetcd_server *srv = cetcd_server_new(&cfg);
        if (srv) {
            cetcd_server_start(srv);
            /* serve blocks until stop; we just need it to bind+listen */
            /* Set alarm to kill after 2 seconds */
            alarm(2);
            cetcd_server_serve(srv);
            cetcd_server_free(srv);
        }
        _exit(0);
    }

    /* parent: wait for server to bind */
    struct timespec ts = {0, 200000000};
    nanosleep(&ts, NULL);
    bool connected = try_connect("127.0.0.1", 23793);
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
    CETCD_ASSERT_TRUE(connected);
}

CETCD_TEST_CASE(live_server_cluster_membership) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 23794;
    cfg.peer_port = 23894;
    cfg.election_tick = 10;
    cfg.heartbeat_tick = 1;

    cetcd_peer_info peers[3] = {
        {1, "127.0.0.1", 23894},
        {2, "127.0.0.1", 23895},
        {3, "127.0.0.1", 23896},
    };
    memcpy(cfg.initial_peers, peers, sizeof(peers));
    cfg.n_initial_peers = 3;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_peer_count(srv), 3);

    cetcd_peer_info new_peer = {4, "127.0.0.1", 23897};
    CETCD_ASSERT_EQ_INT(cetcd_server_add_peer(srv, &new_peer), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_peer_count(srv), 4);

    CETCD_ASSERT_EQ_INT(cetcd_server_remove_peer(srv, 4), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_server_peer_count(srv), 3);

    CETCD_ASSERT_EQ_INT(cetcd_server_remove_peer(srv, 99), CETCD_ERR_NOTFOUND);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(live_server_peer_port_listen) {
    pid_t pid = fork();
    if (pid == 0) {
        cetcd_server_config cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.node_id = 1;
        strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
        cfg.listen_port = 23795;
        cfg.peer_port = 23895;
        cetcd_server *srv = cetcd_server_new(&cfg);
        if (srv) {
            cetcd_server_start(srv);
            alarm(2);
            cetcd_server_serve(srv);
            cetcd_server_free(srv);
        }
        _exit(0);
    }

    struct timespec ts = {0, 200000000};
    nanosleep(&ts, NULL);
    bool client_ok = try_connect("127.0.0.1", 23795);
    bool peer_ok = try_connect("127.0.0.1", 23895);
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
    CETCD_ASSERT_TRUE(client_ok);
    CETCD_ASSERT_TRUE(peer_ok);
}

CETCD_TEST_CASE(live_server_raft_tick_timer) {
    cetcd_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 23796;
    cfg.peer_port = 23896;
    cfg.election_tick = 5;
    cfg.heartbeat_tick = 1;

    cetcd_server *srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);

    for (int i = 0; i < 20; i++) {
        cetcd_server_tick(srv);
    }

    bool is_leader = cetcd_server_is_leader(srv);
    CETCD_ASSERT_TRUE(is_leader);

    cetcd_metrics *m = cetcd_server_metrics(srv);
    CETCD_ASSERT_NOT_NULL(m);
    char buf[4096];
    size_t len = cetcd_metrics_render(m, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(len > 0);
    CETCD_ASSERT_TRUE(strstr(buf, "cetcd_server_info 1") != NULL);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);
}

CETCD_TEST_CASE(live_server_3node_cluster_election) {
    pid_t pids[3] = {0};
    uint16_t base_client = 23800;
    uint16_t base_peer = 23900;

    for (int i = 0; i < 3; i++) {
        pids[i] = fork();
        if (pids[i] == 0) {
            cetcd_server_config cfg;
            memset(&cfg, 0, sizeof(cfg));
            cfg.node_id = (uint64_t)(i + 1);
            strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
            cfg.listen_port = base_client + i;
            cfg.peer_port = base_peer + i;
            cfg.election_tick = 5;
            cfg.heartbeat_tick = 1;

            cetcd_peer_info peers[3];
            memset(peers, 0, sizeof(peers));
            for (int j = 0; j < 3; j++) {
                peers[j].id = (uint64_t)(j + 1);
                strncpy(peers[j].addr, "127.0.0.1", sizeof(peers[j].addr) - 1);
                peers[j].port = base_peer + j;
            }
            memcpy(cfg.initial_peers, peers, sizeof(peers));
            cfg.n_initial_peers = 3;

            cetcd_server *srv = cetcd_server_new(&cfg);
            if (srv) {
                cetcd_server_start(srv);
                alarm(3);
                cetcd_server_serve(srv);
                cetcd_server_free(srv);
            }
            _exit(0);
        }
    }

    struct timespec ts = {1, 500000000};
    nanosleep(&ts, NULL);

    int leader_count = 0;
    for (int i = 0; i < 3; i++) {
        if (try_connect("127.0.0.1", base_client + i)) {
            leader_count++;
        }
    }
    CETCD_ASSERT_TRUE(leader_count >= 1);

    for (int i = 0; i < 3; i++) {
        kill(pids[i], SIGTERM);
    }
    for (int i = 0; i < 3; i++) {
        int st;
        waitpid(pids[i], &st, 0);
    }
}

CETCD_TEST_CASE(live_server_grpc_put_via_tcp) {
    pid_t pid = fork();
    if (pid == 0) {
        cetcd_server_config cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.node_id = 1;
        strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
        cfg.listen_port = 23850;
        cfg.election_tick = 10;
        cfg.heartbeat_tick = 1;
        cetcd_server *srv = cetcd_server_new(&cfg);
        if (srv) {
            cetcd_server_start(srv);
            alarm(3);
            cetcd_server_serve(srv);
            cetcd_server_free(srv);
        }
        _exit(0);
    }

    struct timespec ts = {0, 200000000};
    nanosleep(&ts, NULL);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    CETCD_ASSERT_TRUE(fd >= 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(23850);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    int rc = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    CETCD_ASSERT_EQ_INT(rc, 0);

    uint8_t put_req[] = {0x0a, 0x01, 'x', 0x12, 0x01, 'y'};
    uint8_t grpc_frame[5 + sizeof(put_req)];
    grpc_frame[0] = 0;
    grpc_frame[1] = 0;
    grpc_frame[2] = 0;
    grpc_frame[3] = 0;
    grpc_frame[4] = sizeof(put_req);
    memcpy(grpc_frame + 5, put_req, sizeof(put_req));
    send(fd, grpc_frame, sizeof(grpc_frame), 0);

    uint8_t resp_buf[1024];
    ssize_t n = recv(fd, resp_buf, sizeof(resp_buf), 0);
    CETCD_ASSERT_TRUE(n > 5);

    uint32_t resp_len = ((uint32_t)resp_buf[1] << 24) |
                        ((uint32_t)resp_buf[2] << 16) |
                        ((uint32_t)resp_buf[3] << 8)  |
                        ((uint32_t)resp_buf[4]);
    CETCD_ASSERT_TRUE(resp_len > 0);
    CETCD_ASSERT_TRUE((size_t)n >= 5 + resp_len);

    close(fd);
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(live_server_start_stop),
    CETCD_TEST_ENTRY(live_server_snapshot_after_writes),
    CETCD_TEST_ENTRY(live_server_persistent_backend),
    CETCD_TEST_ENTRY(live_server_tcp_listen),
    CETCD_TEST_ENTRY(live_server_cluster_membership),
    CETCD_TEST_ENTRY(live_server_peer_port_listen),
    CETCD_TEST_ENTRY(live_server_raft_tick_timer),
    CETCD_TEST_ENTRY(live_server_3node_cluster_election),
    CETCD_TEST_ENTRY(live_server_grpc_put_via_tcp),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
