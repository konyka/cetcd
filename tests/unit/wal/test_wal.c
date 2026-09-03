/* Wal encoding/decoding tests - minimal roundtrip */
#define _POSIX_C_SOURCE 200809L
#include "cetcd/wal.h"
#include "cetcd/raft.h"
#include "cetcd_test.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

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
    int r = cetcd_wal_encode_metadata(enc, meta, sizeof(meta));
    CETCD_ASSERT_EQ_INT(r, 0);

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
/* Write a raw WAL frame (8-byte LE header + payload) into fp. */
static void write_raw_frame(FILE *fp, const uint8_t *payload, size_t plen) {
    uint64_t hdr = (uint64_t)plen & 0x00FFFFFFFFFFFFFFULL;
    uint8_t le[8];
    for (int i = 0; i < 8; ++i) le[i] = (uint8_t)((hdr >> (8 * i)) & 0xFF);
    fwrite(le, 1, 8, fp);
    if (plen) fwrite(payload, 1, plen, fp);
}

CETCD_TEST_CASE(wal_decode_rejects_malformed) {
    char path_template[] = "/tmp/cetcd-test-wal-XXXXXX";
    int fd = mkstemp(path_template);
    CETCD_ASSERT(fd >= 0);
    close(fd);

    /* Frame: field 1 (type, wire 0) tag 0x08, then an unterminated varint
     * (all continuation bytes) that runs off the end of the frame. The
     * decoder must reject this rather than accept it as a zero record. */
    FILE *fp = fopen(path_template, "wb");
    CETCD_ASSERT_NOT_NULL(fp);
    const uint8_t bad[] = { 0x08, 0x80, 0x80, 0x80 };
    write_raw_frame(fp, bad, sizeof(bad));
    fclose(fp);

    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path_template);
    CETCD_ASSERT_NOT_NULL(dec);
    cetcd_wal_record rec; cetcd_wal_record_init(&rec);
    int rc = cetcd_wal_decode(dec, &rec);
    CETCD_ASSERT_TRUE(rc != 0);
    cetcd_wal_record_free(&rec);
    cetcd_wal_decoder_free(dec);
    remove(path_template);
}

CETCD_TEST_CASE(wal_decode_rejects_len_overrun) {
    char path_template[] = "/tmp/cetcd-test-wal-XXXXXX";
    int fd = mkstemp(path_template);
    CETCD_ASSERT(fd >= 0);
    close(fd);

    /* Field 7, wire 2 (length-delimited, unknown) claiming 0x10 bytes but
     * providing none. The decoder must not advance pos past the frame. */
    FILE *fp = fopen(path_template, "wb");
    CETCD_ASSERT_NOT_NULL(fp);
    const uint8_t bad[] = { 0x3A, 0x10 };
    write_raw_frame(fp, bad, sizeof(bad));
    fclose(fp);

    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path_template);
    CETCD_ASSERT_NOT_NULL(dec);
    cetcd_wal_record rec; cetcd_wal_record_init(&rec);
    int rc = cetcd_wal_decode(dec, &rec);
    CETCD_ASSERT_TRUE(rc != 0);
    cetcd_wal_record_free(&rec);
    cetcd_wal_decoder_free(dec);
    remove(path_template);
}

CETCD_TEST_CASE(wal_decode_rejects_bad_crc) {
    char path_template[] = "/tmp/cetcd-test-wal-XXXXXX";
    int fd = mkstemp(path_template);
    CETCD_ASSERT(fd >= 0);
    close(fd);

    /* Well-formed record with a wrong CRC. fields: type=METADATA(1),
     * crc=42 (single-byte varint, must terminate), data="abcd".
     * The computed CRC32C of "abcd" is not 42, so decode returns -2. */
    FILE *fp = fopen(path_template, "wb");
    CETCD_ASSERT_NOT_NULL(fp);
    uint8_t rec[10];
    rec[0] = 0x08; rec[1] = 0x01;
    rec[2] = 0x10; rec[3] = 0x2A;
    rec[4] = 0x1A; rec[5] = 0x04; rec[6] = 'a'; rec[7] = 'b'; rec[8] = 'c'; rec[9] = 'd';
    write_raw_frame(fp, rec, 10);
    fclose(fp);

    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path_template);
    CETCD_ASSERT_NOT_NULL(dec);
    cetcd_wal_record rec_out; cetcd_wal_record_init(&rec_out);
    int rc = cetcd_wal_decode(dec, &rec_out);
    CETCD_ASSERT_EQ_INT(rc, -2);
    cetcd_wal_record_free(&rec_out);
    cetcd_wal_decoder_free(dec);
    remove(path_template);
}

CETCD_TEST_CASE(wal_decode_entry_roundtrip) {
    char path_template[] = "/tmp/cetcd-test-wal-ent-XXXXXX";
    int fd = mkstemp(path_template);
    CETCD_ASSERT(fd >= 0);
    close(fd);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(path_template);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry ent;
    memset(&ent, 0, sizeof(ent));
    ent.term = 7;
    ent.index = 9;
    ent.type = CETCD_ENTRY_NORMAL;
    uint8_t payload[] = {1, 'k', 1, 'v', 0};
    ent.data = cetcd_slice_make(payload, sizeof(payload));
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &ent), 0);
    cetcd_wal_encoder_sync(enc);
    cetcd_wal_encoder_free(enc);

    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path_template);
    CETCD_ASSERT_NOT_NULL(dec);
    cetcd_wal_record rec;
    cetcd_wal_record_init(&rec);
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode(dec, &rec), 0);
    CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_ENTRY);
    cetcd_entry out;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode_entry(rec.data, rec.data_len, &out), 0);
    CETCD_ASSERT_TRUE(out.term == 7);
    CETCD_ASSERT_TRUE(out.index == 9);
    CETCD_ASSERT_EQ_INT((int)out.type, CETCD_ENTRY_NORMAL);
    CETCD_ASSERT_EQ_INT((int)out.data.len, (int)sizeof(payload));
    CETCD_ASSERT_EQ_INT(memcmp(out.data.data, payload, sizeof(payload)), 0);
    free((void *)(uintptr_t)out.data.data);
    cetcd_wal_record_free(&rec);
    cetcd_wal_decoder_free(dec);
    remove(path_template);
}

CETCD_TEST_CASE(wal_append_does_not_truncate) {
    char path_template[] = "/tmp/cetcd-test-wal-ap-XXXXXX";
    int fd = mkstemp(path_template);
    CETCD_ASSERT(fd >= 0);
    close(fd);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(path_template);
    const uint8_t meta[] = {'m'};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_metadata(enc, meta, 1), 0);
    cetcd_wal_encoder_free(enc);

    enc = cetcd_wal_encoder_create(path_template);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_hard_state hs = {1, 1, 1};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    cetcd_wal_encoder_free(enc);

    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path_template);
    cetcd_wal_record rec;
    int n = 0;
    while (cetcd_wal_decode(dec, &rec) == 0) {
        n++;
        cetcd_wal_record_free(&rec);
    }
    cetcd_wal_decoder_free(dec);
    remove(path_template);
    CETCD_ASSERT_EQ_INT(n, 2);
}

CETCD_TEST_CASE(wal_directory_segment_path) {
    char dir[] = "/tmp/cetcd-test-waldir-XXXXXX";
    CETCD_ASSERT_NOT_NULL(mkdtemp(dir));
    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(dir);
    CETCD_ASSERT_NOT_NULL(enc);
    const uint8_t meta[] = {'d'};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_metadata(enc, meta, 1), 0);
    cetcd_wal_encoder_free(enc);

    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(dir);
    CETCD_ASSERT_NOT_NULL(dec);
    cetcd_wal_record rec;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode(dec, &rec), 0);
    CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_METADATA);
    cetcd_wal_record_free(&rec);
    cetcd_wal_decoder_free(dec);
}

CETCD_TEST_CASE(wal_decode_hard_state) {
    char path_template[] = "/tmp/cetcd-test-wal-hs-XXXXXX";
    int fd = mkstemp(path_template);
    CETCD_ASSERT(fd >= 0);
    close(fd);
    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(path_template);
    cetcd_hard_state hs = {4, 5, 6};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    cetcd_wal_encoder_free(enc);
    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path_template);
    cetcd_wal_record rec;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode(dec, &rec), 0);
    cetcd_hard_state out;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode_hard_state(rec.data, rec.data_len, &out), 0);
    CETCD_ASSERT_TRUE(out.term == 4 && out.vote == 5 && out.commit == 6);
    cetcd_wal_record_free(&rec);
    cetcd_wal_decoder_free(dec);
    remove(path_template);
}

CETCD_TEST_CASE(wal_snapshot_release_drops_entries) {
    char path_template[] = "/tmp/cetcd-test-wal-rel-XXXXXX";
    int fd = mkstemp(path_template);
    CETCD_ASSERT(fd >= 0);
    close(fd);

    cetcd_wal_encoder *enc = cetcd_wal_encoder_create(path_template);
    CETCD_ASSERT_NOT_NULL(enc);
    cetcd_entry ent;
    memset(&ent, 0, sizeof(ent));
    ent.term = 1;
    ent.index = 1;
    ent.type = CETCD_ENTRY_NORMAL;
    uint8_t payload[] = {1, 2, 3};
    ent.data = cetcd_slice_make(payload, sizeof(payload));
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_entry(enc, &ent), 0);
    cetcd_hard_state hs = {1, 1, 1};
    CETCD_ASSERT_EQ_INT(cetcd_wal_encode_hard_state(enc, &hs), 0);
    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_sync(enc), 0);

    CETCD_ASSERT_EQ_INT(cetcd_wal_encoder_release(enc, 1, 1, &hs), 0);
    cetcd_wal_encoder_free(enc);

    cetcd_wal_decoder *dec = cetcd_wal_decoder_open(path_template);
    CETCD_ASSERT_NOT_NULL(dec);
    cetcd_wal_record rec;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode(dec, &rec), 0);
    CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_SNAPSHOT);
    uint64_t idx = 0, term = 0;
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode_snapshot(rec.data, rec.data_len, &idx, &term), 0);
    CETCD_ASSERT_TRUE(idx == 1 && term == 1);
    cetcd_wal_record_free(&rec);
    CETCD_ASSERT_EQ_INT(cetcd_wal_decode(dec, &rec), 0);
    CETCD_ASSERT_EQ_INT(rec.type, CETCD_WAL_STATE);
    cetcd_wal_record_free(&rec);
    CETCD_ASSERT_TRUE(cetcd_wal_decode(dec, &rec) != 0);
    cetcd_wal_decoder_free(dec);
    remove(path_template);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(wal_roundtrip_basic),
    CETCD_TEST_ENTRY(wal_decode_rejects_malformed),
    CETCD_TEST_ENTRY(wal_decode_rejects_len_overrun),
    CETCD_TEST_ENTRY(wal_decode_rejects_bad_crc),
    CETCD_TEST_ENTRY(wal_decode_entry_roundtrip),
    CETCD_TEST_ENTRY(wal_append_does_not_truncate),
    CETCD_TEST_ENTRY(wal_directory_segment_path),
    CETCD_TEST_ENTRY(wal_decode_hard_state),
    CETCD_TEST_ENTRY(wal_snapshot_release_drops_entries),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
