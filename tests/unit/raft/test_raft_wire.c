#include "cetcd/raft.h"
#include "cetcd_test.h"

CETCD_TEST_CASE(raft_msg_encode_decode_simple) {
    cetcd_msg msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = CETCD_MSG_VOTE;
    msg.to = 2;
    msg.from = 1;
    msg.term = 5;
    msg.log_term = 4;
    msg.index = 10;
    msg.reject = 0;

    uint8_t *buf = NULL;
    size_t len = cetcd_msg_encode_wire(&msg, &buf);
    CETCD_ASSERT_TRUE(len > 0);
    CETCD_ASSERT_NOT_NULL(buf);

    cetcd_msg *out = cetcd_msg_decode_wire(buf, len);
    CETCD_ASSERT_NOT_NULL(out);
    CETCD_ASSERT_EQ_INT(out->type, CETCD_MSG_VOTE);
    CETCD_ASSERT_EQ_INT((int)out->to, 2);
    CETCD_ASSERT_EQ_INT((int)out->from, 1);
    CETCD_ASSERT_EQ_INT((int)out->term, 5);
    CETCD_ASSERT_EQ_INT((int)out->log_term, 4);
    CETCD_ASSERT_EQ_INT((int)out->index, 10);
    CETCD_ASSERT_EQ_INT((int)out->reject, 0);

    free(buf);
    cetcd_msg_free(out);
}

CETCD_TEST_CASE(raft_msg_encode_decode_with_entries) {
    cetcd_msg msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = CETCD_MSG_APP;
    msg.to = 3;
    msg.from = 1;
    msg.term = 2;
    msg.commit = 5;

    uint8_t entry_data[] = {0x0a, 0x01, 'k', 0x12, 0x01, 'v'};
    cetcd_entry entries[2];
    memset(entries, 0, sizeof(entries));
    entries[0].term = 2;
    entries[0].index = 3;
    entries[0].type = CETCD_ENTRY_NORMAL;
    entries[0].data.data = entry_data;
    entries[0].data.len = sizeof(entry_data);

    entries[1].term = 2;
    entries[1].index = 4;
    entries[1].type = CETCD_ENTRY_NORMAL;
    entries[1].data.data = NULL;
    entries[1].data.len = 0;

    msg.entries = entries;
    msg.n_entries = 2;

    uint8_t *buf = NULL;
    size_t len = cetcd_msg_encode_wire(&msg, &buf);
    CETCD_ASSERT_TRUE(len > 0);
    CETCD_ASSERT_NOT_NULL(buf);

    cetcd_msg *out = cetcd_msg_decode_wire(buf, len);
    CETCD_ASSERT_NOT_NULL(out);
    CETCD_ASSERT_EQ_INT(out->type, CETCD_MSG_APP);
    CETCD_ASSERT_EQ_INT((int)out->n_entries, 2);
    CETCD_ASSERT_EQ_INT((int)out->entries[0].term, 2);
    CETCD_ASSERT_EQ_INT((int)out->entries[0].index, 3);
    CETCD_ASSERT_EQ_INT((int)out->entries[0].data.len, 6);
    CETCD_ASSERT_EQ_INT(out->entries[0].data.data[2], 'k');
    CETCD_ASSERT_EQ_INT((int)out->entries[1].index, 4);

    free(buf);
    cetcd_msg_free(out);
}

CETCD_TEST_CASE(raft_msg_encode_decode_roundtrip_all_types) {
    cetcd_msg_type types[] = {
        CETCD_MSG_HUP, CETCD_MSG_BEAT, CETCD_MSG_PROP, CETCD_MSG_APP,
        CETCD_MSG_APP_RESP, CETCD_MSG_VOTE, CETCD_MSG_VOTE_RESP,
        CETCD_MSG_SNAP, CETCD_MSG_HEARTBEAT, CETCD_MSG_HEARTBEAT_RESP,
        CETCD_MSG_UNREACHABLE, CETCD_MSG_SNAP_STATUS, CETCD_MSG_CHECK_QUORUM,
        CETCD_MSG_TRANSFER_LEADER, CETCD_MSG_TIMEOUT_NOW,
        CETCD_MSG_READ_INDEX, CETCD_MSG_READ_INDEX_RESP,
        CETCD_MSG_PRE_VOTE, CETCD_MSG_PRE_VOTE_RESP,
    };
    int n = (int)(sizeof(types) / sizeof(types[0]));

    for (int i = 0; i < n; i++) {
        cetcd_msg msg;
        memset(&msg, 0, sizeof(msg));
        msg.type = types[i];
        msg.to = (uint64_t)(i + 1);
        msg.from = 42;
        msg.term = (uint64_t)(i * 100);
        msg.commit = (uint64_t)(i * 10);

        uint8_t *buf = NULL;
        size_t len = cetcd_msg_encode_wire(&msg, &buf);
        CETCD_ASSERT_TRUE(len > 0);

        cetcd_msg *out = cetcd_msg_decode_wire(buf, len);
        CETCD_ASSERT_NOT_NULL(out);
        CETCD_ASSERT_EQ_INT(out->type, types[i]);
        CETCD_ASSERT_EQ_INT((int)out->to, (int)(i + 1));
        CETCD_ASSERT_EQ_INT((int)out->from, 42);

        free(buf);
        cetcd_msg_free(out);
    }
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(raft_msg_encode_decode_simple),
    CETCD_TEST_ENTRY(raft_msg_encode_decode_with_entries),
    CETCD_TEST_ENTRY(raft_msg_encode_decode_roundtrip_all_types),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
