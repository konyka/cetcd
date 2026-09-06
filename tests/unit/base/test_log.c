#include "cetcd/base.h"
#include "cetcd_test.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if !defined(_WIN32)
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <sys/time.h>
#endif

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

CETCD_TEST_CASE(log_outputs_stdio_and_file) {
    FILE *owned = (FILE *)1;
    CETCD_ASSERT_EQ_INT(cetcd_log_open_outputs("stderr", &owned), 0);
    CETCD_ASSERT_TRUE(owned == NULL);
    CETCD_ASSERT_TRUE(cetcd_log_get_sink() == stderr);

    CETCD_ASSERT_EQ_INT(cetcd_log_open_outputs("stdout", &owned), 0);
    CETCD_ASSERT_TRUE(cetcd_log_get_sink() == stdout);

    CETCD_ASSERT_EQ_INT(cetcd_log_open_outputs("stderr,stderr", &owned), 0);
    CETCD_ASSERT_TRUE(cetcd_log_get_sink() == stderr);

    char path[256];
    snprintf(path, sizeof(path), "cetcd-log-out-%u.txt",
             (unsigned)((uintptr_t)&path & 0xFFFFFFFFu));
    remove(path);
    owned = NULL;
    CETCD_ASSERT_EQ_INT(cetcd_log_open_outputs(path, &owned), 0);
    CETCD_ASSERT_NOT_NULL(owned);
    CETCD_INFO("file-sink");
    fclose(owned);
    cetcd_log_set_sink(stderr);

    FILE *fp = fopen(path, "r");
    CETCD_ASSERT_NOT_NULL(fp);
    char buf[256] = {0};
    CETCD_ASSERT_TRUE(fread(buf, 1, sizeof(buf) - 1, fp) > 0);
    fclose(fp);
    remove(path);
    CETCD_ASSERT_TRUE(strstr(buf, "file-sink") != NULL);
}

CETCD_TEST_CASE(log_outputs_fail_closed) {
    FILE *owned = NULL;
    CETCD_ASSERT_TRUE(cetcd_log_open_outputs(NULL, &owned) != 0);
    CETCD_ASSERT_TRUE(cetcd_log_open_outputs("", &owned) != 0);
    CETCD_ASSERT_TRUE(cetcd_log_open_journal("/no/such/cetcd-journal.sock", &owned) != 0);
#if defined(_WIN32)
    CETCD_ASSERT_TRUE(cetcd_log_open_outputs("journal", &owned) != 0);
    CETCD_ASSERT_TRUE(cetcd_log_open_outputs("syslog", &owned) != 0);
#endif
    CETCD_ASSERT_TRUE(cetcd_log_open_outputs("stderr,stdout", &owned) != 0);
    CETCD_ASSERT_TRUE(cetcd_log_open_outputs("stderr,/tmp/cetcd-mixed.log", &owned) != 0);
    CETCD_ASSERT_TRUE(cetcd_log_open_outputs("stderr,journal", &owned) != 0);
    cetcd_log_set_sink(stderr);
}

#if !defined(_WIN32)
CETCD_TEST_CASE(log_outputs_journal_unix_socket) {
    char path[128];
    snprintf(path, sizeof(path), "/tmp/cetcd-jnl-%u.sock", (unsigned)getpid());
    unlink(path);
    int srv = socket(AF_UNIX, SOCK_DGRAM, 0);
    CETCD_ASSERT_TRUE(srv >= 0);
    struct sockaddr_un un;
    memset(&un, 0, sizeof(un));
    un.sun_family = AF_UNIX;
    strncpy(un.sun_path, path, sizeof(un.sun_path) - 1);
    CETCD_ASSERT_EQ_INT(bind(srv, (struct sockaddr *)&un, sizeof(un)), 0);
    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
    CETCD_ASSERT_EQ_INT(setsockopt(srv, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)), 0);

    FILE *owned = NULL;
    CETCD_ASSERT_EQ_INT(cetcd_log_open_journal(path, &owned), 0);
    CETCD_ASSERT_NOT_NULL(owned);
    CETCD_INFO("journal-line");
    fflush(owned);

    char buf[256] = {0};
    ssize_t n = recv(srv, buf, sizeof(buf) - 1, 0);
    CETCD_ASSERT_TRUE(n > 0);
    buf[n] = '\0';
    CETCD_ASSERT_TRUE(strstr(buf, "journal-line") != NULL);

    fclose(owned);
    cetcd_log_set_sink(stderr);
    close(srv);
    unlink(path);
}
#endif

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
    CETCD_TEST_ENTRY(log_outputs_stdio_and_file),
    CETCD_TEST_ENTRY(log_outputs_fail_closed),
#if !defined(_WIN32)
    CETCD_TEST_ENTRY(log_outputs_journal_unix_socket),
#endif
    CETCD_TEST_ENTRY(level_name_lookup),
CETCD_TEST_LIST_END
CETCD_TEST_MAIN()
