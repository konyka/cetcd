#include "cetcd/peer.h"
#include "cetcd_test.h"

#include <string.h>

CETCD_TEST_CASE(reconfig_may_remove_learner_always) {
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(1, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(2, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(0, 1), 1);
}

CETCD_TEST_CASE(reconfig_may_remove_keeps_quorum) {
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(0, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(1, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(2, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(3, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(4, 0), 1);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_remove(5, 0), 1);
}

CETCD_TEST_CASE(cluster_voter_count_self_and_peers) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_voter_count(c), 1);
    cetcd_peer_info v = {.id = 2, .addr = "10.0.0.2", .port = 2380, .is_learner = 0};
    cetcd_peer_info l = {.id = 3, .addr = "10.0.0.3", .port = 2380, .is_learner = 1};
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &v), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &l), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_voter_count(c), 2);
    cetcd_cluster_free(c);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_voter_count(NULL), 0);
}

CETCD_TEST_CASE(cluster_learner_count_peers) {
    cetcd_cluster *c = cetcd_cluster_new(1);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_learner_count(c), 0);
    cetcd_peer_info v = {.id = 2, .addr = "10.0.0.2", .port = 2380, .is_learner = 0};
    cetcd_peer_info l = {.id = 3, .addr = "10.0.0.3", .port = 2380, .is_learner = 1};
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &v), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_cluster_add_peer(c, &l), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_learner_count(c), 1);
    cetcd_cluster_free(c);
    CETCD_ASSERT_EQ_INT((int)cetcd_cluster_learner_count(NULL), 0);
}

CETCD_TEST_CASE(reconfig_may_add_learner_cap) {
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_add_learner(0, 1), 1);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_add_learner(1, 1), 0);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_add_learner(0, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_add_learner(5, 0), 0);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_add_learner(1, 2), 1);
    CETCD_ASSERT_EQ_INT(cetcd_reconfig_may_add_learner(2, 2), 0);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(reconfig_may_remove_learner_always),
    CETCD_TEST_ENTRY(reconfig_may_remove_keeps_quorum),
    CETCD_TEST_ENTRY(cluster_voter_count_self_and_peers),
    CETCD_TEST_ENTRY(cluster_learner_count_peers),
    CETCD_TEST_ENTRY(reconfig_may_add_learner_cap),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
