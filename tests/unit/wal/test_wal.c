/* Wal encoding/decoding tests - minimal roundtrip */
#define _POSIX_C_SOURCE 200809L
#include "cetcd/wal.h"
#include "cetcd/raft.h"
#include "cetcd_test.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Helpers to prepare test data */
static cetcd_slice to_slice(const char *s) {
    return cetcd_slice_make(s, s ? strlen(s) : 0);
}

CETCD_TEST_CASE(wal_roundtrip_basic) {
    char path_template[] = "/tmp/cetcd-test-wal-XXXXXX";
    int fd = mkstemp(path_template);
    if (fd < 0) {
        CETCD_ASSERT(false);
    }
    close(fd);
    const char *path = path_template;

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(path);
    CETCD_ASSERT_NOT_NULL(enc);

    /* Metadata */
    const uint8_t meta[] = { 'm','e','t','a' };
    {
        cetcd_wal_record rec; cetcd_wal_record_init(&rec);
        rec.type = CETCD_WAL_METADATA;
        rec.data = (uint8_t*)meta; rec.data_len = sizeof(meta);
        rec.crc = 0;
        int r = cetcd_wal_encode(enc, &rec);
        rec.data = NULL;
        cetcd_wal_record_free(&rec);
        CETCD_ASSERT_EQ_INT(r, 0);
    }

    /* Entry */
    cetcd_entry ent;
    ent.term = 1; ent.index = 2; ent.type = CETCD_ENTRY_NORMAL;
    uint8_t payload[] = "hello";
    ent.data = cetcd_slice_make(payload, sizeof(payload)-1);
    int r2 = cetcd_wal_encode_entry(enc, &ent);
    CETCD_ASSERT_EQ_INT(r2, 0);

    /* Hard state */
    cetcd_hard_state hs = {3, 4, 5};
    int r3 = cetcd_wal_encode_hard_state(enc, &hs);
    CETCD_ASSERT_EQ_INT(r3, 0);

    cetcd_wal_encoder_flush(enc);
    cetcd_wal_encoder_free(enc);

    /* Decode and verify basic structure and CRC correctness */
    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path);
    CETCD_ASSERT_NOT_NULL(dec);

    cetcd_wal_record rec;
    int got = 0;
    /* First frame: metadata */
    if (cetcd_wal_decode(dec, &rec) == 0) {
        CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_METADATA);
        CETCD_ASSERT_NOT_NULL(rec.data);
        CETCD_ASSERT_EQ_INT(rec.data_len, sizeof(meta));
        CETCD_ASSERT_EQ_INT(memcmp(rec.data, meta, sizeof(meta)), 0);
        cetcd_wal_record_free(&rec);
        got++;
    }
    /* Second: entry */
    if (cetcd_wal_decode(dec, &rec) == 0) {
        CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_ENTRY);
        CETCD_ASSERT_NOT_NULL(rec.data);
        /* We can't perfectly verify nested payload here, but ensure data present */
        CETCD_ASSERT_TRUE((int)rec.data_len > 0);
        cetcd_wal_record_free(&rec);
        got++;
    }
    /* Third: hard state */
    if (cetcd_wal_decode(dec, &rec) == 0) {
        CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_STATE);
        CETCD_ASSERT_NOT_NULL(rec.data);
        CETCD_ASSERT_TRUE((int)rec.data_len > 0);
        cetcd_wal_record_free(&rec);
        got++;
    }

    cetcd_wal_decoder_free(dec);
    /* cleanup test file */
    remove(path);
    CETCD_ASSERT_EQ_INT(got, 3);
}

/* Extend to test macro to satisfy harness expectations */
CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(wal_roundtrip_basic),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
