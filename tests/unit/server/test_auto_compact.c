#include "cetcd/server.h"
#include "cetcd_test.h"

#include <string.h>

CETCD_TEST_CASE(auto_compact_parse_mode) {
    cetcd_auto_compact_mode m = CETCD_AUTO_COMPACT_OFF;
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("periodic", &m), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)m, (int)CETCD_AUTO_COMPACT_PERIODIC);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("revision", &m), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)m, (int)CETCD_AUTO_COMPACT_REVISION);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("foo", &m), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("", &m), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode(NULL, &m), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_mode("periodic", NULL),
                        CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_retention_periodic) {
    uint64_t v = 99;
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "0", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 3600);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1h", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 3600);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "30m", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1800);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1h30m", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 5400);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "10s", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 10);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "500ms", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "0s", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "abc", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "-1", CETCD_AUTO_COMPACT_PERIODIC, &v), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_parse_retention_revision) {
    uint64_t v = 0;
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1000", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 1000);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "0", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_OK);
    CETCD_ASSERT_TRUE(v == 0);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1h", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "10s", CETCD_AUTO_COMPACT_REVISION, &v), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_parse_auto_compaction_retention(
        "1", CETCD_AUTO_COMPACT_OFF, &v), CETCD_ERR_INVAL);
}

CETCD_TEST_CASE(auto_compact_due_revision) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    st.mode = CETCD_AUTO_COMPACT_REVISION;
    st.retention = 3;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 2, 0, 0), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 0, 0), 7);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 7, 0), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 12, 7, 0), 9);
}

CETCD_TEST_CASE(auto_compact_due_periodic) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    st.mode = CETCD_AUTO_COMPACT_PERIODIC;
    st.retention = 1;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 5, 0, 100), 0);
    CETCD_ASSERT_EQ_INT((int)st.window_rev, 5);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 8, 0, 500), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 8, 0, 1100), 5);
    CETCD_ASSERT_EQ_INT((int)st.window_rev, 8);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 9, 5, 1200), 0);
}

CETCD_TEST_CASE(auto_compact_due_off) {
    cetcd_auto_compact_state st;
    memset(&st, 0, sizeof(st));
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 0, 1000), 0);
    st.mode = CETCD_AUTO_COMPACT_REVISION;
    st.retention = 0;
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(&st, 10, 0, 1000), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auto_compact_due(NULL, 10, 0, 1000), 0);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(auto_compact_parse_mode),
    CETCD_TEST_ENTRY(auto_compact_parse_retention_periodic),
    CETCD_TEST_ENTRY(auto_compact_parse_retention_revision),
    CETCD_TEST_ENTRY(auto_compact_due_revision),
    CETCD_TEST_ENTRY(auto_compact_due_periodic),
    CETCD_TEST_ENTRY(auto_compact_due_off),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
