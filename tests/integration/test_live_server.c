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
    char data_dir[] = "/tmp/cetcd_test_persist_XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(data_dir));

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

    int64_t rev = cetcd_server_revision(srv);
    CETCD_ASSERT_TRUE(rev > 0);

    cetcd_server_stop(srv);
    cetcd_server_free(srv);

    /* Restart and verify the key survived LMDB persistence. */
    memset(&cfg, 0, sizeof(cfg));
    cfg.node_id = 1;
    cfg.listen_port = 23792;
    strncpy(cfg.data_dir, data_dir, sizeof(cfg.data_dir) - 1);
    srv = cetcd_server_new(&cfg);
    CETCD_ASSERT_NOT_NULL(srv);
    CETCD_ASSERT_EQ_INT(cetcd_server_start(srv), 0);
    CETCD_ASSERT_TRUE(cetcd_server_revision(srv) >= rev);

    uint8_t range_req[] = {0x0a, 0x04, 't','e','s','t'};
    resp = cetcd_server_handle_rpc(srv, "/etcdserverpb.KV/Range",
                                   range_req, sizeof(range_req));
    CETCD_ASSERT_NOT_NULL(resp.data);
    CETCD_ASSERT_TRUE(resp.len > 0);
    /* Response should contain the value "data". */
    int found = 0;
    for (size_t i = 0; i + 4 <= resp.len; i++) {
        if (memcmp(resp.data + i, "data", 4) == 0) { found = 1; break; }
    }
    CETCD_ASSERT_TRUE(found);
    cetcd_server_rpc_result_free(&resp);

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
    cetcd_buf_t buf;
    cetcd_buf_init(&buf);
    CETCD_ASSERT_EQ_INT(cetcd_metrics_render(m, &buf), 0);
    CETCD_ASSERT_TRUE(buf.len > 0);
    CETCD_ASSERT_TRUE(strstr((char *)buf.data, "cetcd_server_info 1") != NULL);
    cetcd_buf_free(&buf);

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

static size_t build_grpc_request(uint8_t *out, size_t out_size,
                                  const char *path,
                                  const uint8_t *payload, size_t payload_len) {
    size_t path_len = strlen(path);
    size_t pos = 0;
    out[pos++] = (uint8_t)(path_len >> 8);
    out[pos++] = (uint8_t)(path_len & 0xFF);
    memcpy(out + pos, path, path_len);
    pos += path_len;
    out[pos++] = 0;
    out[pos++] = (uint8_t)((payload_len >> 24) & 0xFF);
    out[pos++] = (uint8_t)((payload_len >> 16) & 0xFF);
    out[pos++] = (uint8_t)((payload_len >> 8) & 0xFF);
    out[pos++] = (uint8_t)(payload_len & 0xFF);
    memcpy(out + pos, payload, payload_len);
    pos += payload_len;
    return pos;
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
    uint8_t grpc_frame[4096];
    size_t frame_len = build_grpc_request(grpc_frame, sizeof(grpc_frame),
                                           "/etcdserverpb.KV/Put",
                                           put_req, sizeof(put_req));
    send(fd, grpc_frame, frame_len, 0);

    uint8_t resp_buf[1024];
    ssize_t n = recv(fd, resp_buf, sizeof(resp_buf), 0);
    CETCD_ASSERT_TRUE(n > 7);

    uint16_t rpath_len = ((uint16_t)resp_buf[0] << 8) | resp_buf[1];
    CETCD_ASSERT_TRUE(rpath_len > 0);

    uint32_t resp_payload = ((uint32_t)resp_buf[2 + rpath_len + 1] << 24) |
                            ((uint32_t)resp_buf[2 + rpath_len + 2] << 16) |
                            ((uint32_t)resp_buf[2 + rpath_len + 3] << 8)  |
                            ((uint32_t)resp_buf[2 + rpath_len + 4]);
    CETCD_ASSERT_TRUE(resp_payload > 0);

    close(fd);
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
}

CETCD_TEST_CASE(live_server_grpc_put_range_roundtrip) {
    pid_t pid = fork();
    if (pid == 0) {
        cetcd_server_config cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.node_id = 1;
        strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
        cfg.listen_port = 23851;
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
    sa.sin_port = htons(23851);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    CETCD_ASSERT_EQ_INT(connect(fd, (struct sockaddr *)&sa, sizeof(sa)), 0);

    uint8_t put_req[] = {0x0a, 0x03, 'k', 'e', 'y', 0x12, 0x03, 'v', 'a', 'l'};
    uint8_t frame_buf[4096];
    size_t put_frame_len = build_grpc_request(frame_buf, sizeof(frame_buf),
                                               "/etcdserverpb.KV/Put",
                                               put_req, sizeof(put_req));
    send(fd, frame_buf, put_frame_len, 0);

    uint8_t resp1[1024];
    ssize_t n1 = recv(fd, resp1, sizeof(resp1), 0);
    CETCD_ASSERT_TRUE(n1 > 7);

    uint8_t range_req[] = {0x0a, 0x03, 'k', 'e', 'y'};
    size_t range_frame_len = build_grpc_request(frame_buf, sizeof(frame_buf),
                                                  "/etcdserverpb.KV/Range",
                                                  range_req, sizeof(range_req));
    send(fd, frame_buf, range_frame_len, 0);

    uint8_t resp2[2048];
    ssize_t n2 = recv(fd, resp2, sizeof(resp2), 0);
    CETCD_ASSERT_TRUE(n2 > 7);

    uint16_t rpath2 = ((uint16_t)resp2[0] << 8) | resp2[1];
    uint32_t resp2_payload = ((uint32_t)resp2[2 + rpath2 + 1] << 24) |
                             ((uint32_t)resp2[2 + rpath2 + 2] << 16) |
                             ((uint32_t)resp2[2 + rpath2 + 3] << 8)  |
                             ((uint32_t)resp2[2 + rpath2 + 4]);
    CETCD_ASSERT_TRUE(resp2_payload > 0);

    close(fd);
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
}

CETCD_TEST_CASE(live_server_grpc_delete_range) {
    pid_t pid = fork();
    if (pid == 0) {
        cetcd_server_config cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.node_id = 1;
        strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
        cfg.listen_port = 23852;
        cfg.election_tick = 10;
        cfg.heartbeat_tick = 1;
        cetcd_server *srv = cetcd_server_new(&cfg);
        if (srv) { cetcd_server_start(srv); alarm(3); cetcd_server_serve(srv); cetcd_server_free(srv); }
        _exit(0);
    }

    struct timespec ts = {0, 200000000};
    nanosleep(&ts, NULL);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    CETCD_ASSERT_TRUE(fd >= 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(23852);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    CETCD_ASSERT_EQ_INT(connect(fd, (struct sockaddr *)&sa, sizeof(sa)), 0);

    uint8_t put_req[] = {0x0a, 0x02, 'd', 'k', 0x12, 0x02, 'd', 'v'};
    uint8_t frame_buf[4096];
    size_t put_len = build_grpc_request(frame_buf, sizeof(frame_buf),
                                          "/etcdserverpb.KV/Put", put_req, sizeof(put_req));
    send(fd, frame_buf, put_len, 0);
    uint8_t r1[1024];
    CETCD_ASSERT_TRUE(recv(fd, r1, sizeof(r1), 0) > 7);

    uint8_t del_req[] = {0x0a, 0x02, 'd', 'k'};
    size_t del_len = build_grpc_request(frame_buf, sizeof(frame_buf),
                                           "/etcdserverpb.KV/DeleteRange", del_req, sizeof(del_req));
    send(fd, frame_buf, del_len, 0);
    uint8_t r2[1024];
    ssize_t n2 = recv(fd, r2, sizeof(r2), 0);
    CETCD_ASSERT_TRUE(n2 > 7);

    uint16_t rpath2 = ((uint16_t)r2[0] << 8) | r2[1];
    uint32_t r2_payload = ((uint32_t)r2[2 + rpath2 + 1] << 24) |
                          ((uint32_t)r2[2 + rpath2 + 2] << 16) |
                          ((uint32_t)r2[2 + rpath2 + 3] << 8)  |
                          ((uint32_t)r2[2 + rpath2 + 4]);
    CETCD_ASSERT_TRUE(r2_payload > 0);

    close(fd);
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
}

CETCD_TEST_CASE(live_server_grpc_lease_grant) {
    pid_t pid = fork();
    if (pid == 0) {
        cetcd_server_config cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.node_id = 1;
        strncpy(cfg.listen_addr, "127.0.0.1", sizeof(cfg.listen_addr) - 1);
        cfg.listen_port = 23853;
        cfg.election_tick = 10;
        cfg.heartbeat_tick = 1;
        cetcd_server *srv = cetcd_server_new(&cfg);
        if (srv) { cetcd_server_start(srv); alarm(3); cetcd_server_serve(srv); cetcd_server_free(srv); }
        _exit(0);
    }

    struct timespec ts = {0, 200000000};
    nanosleep(&ts, NULL);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    CETCD_ASSERT_TRUE(fd >= 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(23853);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    CETCD_ASSERT_EQ_INT(connect(fd, (struct sockaddr *)&sa, sizeof(sa)), 0);

    uint8_t lease_req[] = {0x08, 0x3c};
    uint8_t frame_buf[4096];
    size_t lease_len = build_grpc_request(frame_buf, sizeof(frame_buf),
                                            "/etcdserverpb.Lease/LeaseGrant", lease_req, sizeof(lease_req));
    send(fd, frame_buf, lease_len, 0);
    uint8_t r[1024];
    ssize_t n = recv(fd, r, sizeof(r), 0);
    CETCD_ASSERT_TRUE(n > 7);

    uint16_t rpath = ((uint16_t)r[0] << 8) | r[1];
    uint32_t r_payload = ((uint32_t)r[2 + rpath + 1] << 24) |
                         ((uint32_t)r[2 + rpath + 2] << 16) |
                         ((uint32_t)r[2 + rpath + 3] << 8)  |
                         ((uint32_t)r[2 + rpath + 4]);
    CETCD_ASSERT_TRUE(r_payload > 0);

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
    CETCD_TEST_ENTRY(live_server_grpc_put_range_roundtrip),
    CETCD_TEST_ENTRY(live_server_grpc_delete_range),
    CETCD_TEST_ENTRY(live_server_grpc_lease_grant),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
