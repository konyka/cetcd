#include "cetcd/base.h"
#include "cetcd_test.h"

#include <stdio.h>
#include <string.h>

static FILE *open_tmpsink(void) {
    FILE *fp = tmpfile();
    CETCD_ASSERT_NOT_NULL(fp);
    return fp;
}

static size_t drain_to_buf(FILE *fp, char *dst, size_t cap) {
    fflush(fp);
    rewind(fp);
    size_t n = fread(dst, 1, cap - 1, fp);
    dst[n] = '\0';
    return n;
}

CETCD_TEST_CASE(level_set_get_roundtrips) {
    cetcd_log_set_level(CETCD_LOG_DEBUG);
    CETCD_ASSERT_EQ_INT(cetcd_log_get_level(), CETCD_LOG_DEBUG);
    cetcd_log_set_level(CETCD_LOG_WARN);
    CETCD_ASSERT_EQ_INT(cetcd_log_get_level(), CETCD_LOG_WARN);
}

CETCD_TEST_CASE(filter_below_level_is_dropped) {
    FILE *fp = open_tmpsink();
    cetcd_log_set_sink(fp);
    cetcd_log_set_level(CETCD_LOG_WARN);
    cetcd_log_set_format(CETCD_LOG_FORMAT_TEXT);

    CETCD_INFO("invisible: %d", 1);

    char buf[256] = {0};
    drain_to_buf(fp, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(strstr(buf, "invisible") == NULL);

    fclose(fp);
    cetcd_log_set_sink(stderr);
}

CETCD_TEST_CASE(emit_at_or_above_level_appears) {
    FILE *fp = open_tmpsink();
    cetcd_log_set_sink(fp);
    cetcd_log_set_level(CETCD_LOG_INFO);
    cetcd_log_set_format(CETCD_LOG_FORMAT_TEXT);

    CETCD_WARN("visible-%d", 7);

    char buf[256] = {0};
    drain_to_buf(fp, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(strstr(buf, "visible-7") != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "WARN") != NULL);

    fclose(fp);
    cetcd_log_set_sink(stderr);
}

CETCD_TEST_CASE(json_format_emits_valid_ish_json) {
    FILE *fp = open_tmpsink();
    cetcd_log_set_sink(fp);
    cetcd_log_set_level(CETCD_LOG_INFO);
    cetcd_log_set_format(CETCD_LOG_FORMAT_JSON);

    CETCD_INFO("json-%s", "ok");

    char buf[512] = {0};
    drain_to_buf(fp, buf, sizeof(buf));
    CETCD_ASSERT_TRUE(buf[0] == '{');
    CETCD_ASSERT_TRUE(strstr(buf, "\"level\"") != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "\"msg\"")   != NULL);
    CETCD_ASSERT_TRUE(strstr(buf, "json-ok")   != NULL);

    fclose(fp);
    cetcd_log_set_sink(stderr);
    cetcd_log_set_format(CETCD_LOG_FORMAT_TEXT);
}

CETCD_TEST_CASE(level_name_lookup) {
    CETCD_ASSERT_EQ_STR(cetcd_log_level_name(CETCD_LOG_TRACE), "TRACE");
    CETCD_ASSERT_EQ_STR(cetcd_log_level_name(CETCD_LOG_DEBUG), "DEBUG");
    CETCD_ASSERT_EQ_STR(cetcd_log_level_name(CETCD_LOG_INFO),  "INFO");
    CETCD_ASSERT_EQ_STR(cetcd_log_level_name(CETCD_LOG_WARN),  "WARN");
    CETCD_ASSERT_EQ_STR(cetcd_log_level_name(CETCD_LOG_ERROR), "ERROR");
    CETCD_ASSERT_EQ_STR(cetcd_log_level_name(CETCD_LOG_FATAL), "FATAL");
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(level_set_get_roundtrips),
    CETCD_TEST_ENTRY(filter_below_level_is_dropped),
    CETCD_TEST_ENTRY(emit_at_or_above_level_appears),
    CETCD_TEST_ENTRY(json_format_emits_valid_ish_json),
    CETCD_TEST_ENTRY(level_name_lookup),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
