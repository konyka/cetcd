#include "cetcd/peer.h"
#include "cetcd_test.h"

#include <stdio.h>
#include <string.h>

CETCD_TEST_CASE(parse_two_peers_http) {
    cetcd_peer_info peers[8];
    uint32_t n = 0;
    int https = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=http://127.0.0.1:2380,2=http://127.0.0.1:2382",
        peers, 8, &n, &https), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 2);
    CETCD_ASSERT_EQ_INT(https, 0);
    CETCD_ASSERT_EQ_INT((int)peers[0].id, 1);
    CETCD_ASSERT_EQ_INT((int)peers[0].port, 2380);
    CETCD_ASSERT_TRUE(strcmp(peers[0].addr, "127.0.0.1") == 0);
    CETCD_ASSERT_EQ_INT((int)peers[1].id, 2);
    CETCD_ASSERT_EQ_INT((int)peers[1].port, 2382);
}

CETCD_TEST_CASE(parse_https_sets_flag) {
    cetcd_peer_info peers[4];
    uint32_t n = 0;
    int https = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=https://127.0.0.1:2380",
        peers, 4, &n, &https), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 1);
    CETCD_ASSERT_EQ_INT(https, 1);
    CETCD_ASSERT_TRUE(strcmp(peers[0].addr, "127.0.0.1") == 0);
}

CETCD_TEST_CASE(parse_default_port_is_2380) {
    cetcd_peer_info peers[2];
    uint32_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "3=127.0.0.1", peers, 2, &n, NULL), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)n, 1);
    CETCD_ASSERT_EQ_INT((int)peers[0].id, 3);
    CETCD_ASSERT_EQ_INT((int)peers[0].port, 2380);
}

CETCD_TEST_CASE(parse_rejects_id_zero_and_name) {
    cetcd_peer_info peers[2];
    uint32_t n = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "0=127.0.0.1:2380", peers, 2, &n, NULL), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT((int)n, 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "node1=127.0.0.1:2380", peers, 2, &n, NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(parse_rejects_bad_port) {
    cetcd_peer_info peers[2];
    uint32_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=127.0.0.1:0", peers, 2, &n, NULL), CETCD_ERR_RANGE);
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=127.0.0.1:abc", peers, 2, &n, NULL), CETCD_ERR_RANGE);
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=127.0.0.1:65536", peers, 2, &n, NULL), CETCD_ERR_RANGE);
}

CETCD_TEST_CASE(parse_rejects_empty_duplicate_overflow) {
    cetcd_peer_info peers[2];
    uint32_t n = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "", peers, 2, &n, NULL), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        NULL, peers, 2, &n, NULL), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=127.0.0.1:2380,,2=127.0.0.1:2382", peers, 2, &n, NULL),
        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=127.0.0.1:2380,1=10.0.0.1:2380", peers, 2, &n, NULL),
        CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_initial_cluster(
        "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384",
        peers, 2, &n, NULL), CETCD_ERR_INVAL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(parse_two_peers_http),
    CETCD_TEST_ENTRY(parse_https_sets_flag),
    CETCD_TEST_ENTRY(parse_default_port_is_2380),
    CETCD_TEST_ENTRY(parse_rejects_id_zero_and_name),
    CETCD_TEST_ENTRY(parse_rejects_bad_port),
    CETCD_TEST_ENTRY(parse_rejects_empty_duplicate_overflow),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
