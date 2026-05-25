#include "cetcd/base.h"
#include "cetcd/proto.h"
#include "cetcd_test.h"
#include <stdlib.h>
#include <string.h>

/* Access internal kv struct for direct testing */
#include "kv.pb-c.h"

CETCD_TEST_CASE(kv_message_pack_unpack_roundtrip) {
    Etcd__KeyValue *kv = (Etcd__KeyValue *)calloc(1, sizeof(Etcd__KeyValue));
    CETCD_ASSERT_NOT_NULL(kv);
    kv->base.descriptor = &Etcd__KeyValue__descriptor;
    kv->key = (uint8_t *)"my-key";
    kv->key_len = 6;
    kv->value = (uint8_t *)"my-value";
    kv->value_len = 8;
    kv->create_revision = 10;
    kv->mod_revision = 20;
    kv->version = 3;
    kv->lease = 758300;

    size_t sz = cetcd_proto_packed_size((ProtobufCMessage *)kv);
    CETCD_ASSERT_TRUE(sz > 0);

    uint8_t *buf = (uint8_t *)malloc(sz);
    CETCD_ASSERT_NOT_NULL(buf);
    size_t packed = cetcd_proto_pack((ProtobufCMessage *)kv, buf, sz);
    CETCD_ASSERT_TRUE(packed == sz);

    ProtobufCMessage *raw = cetcd_proto_unpack(&Etcd__KeyValue__descriptor,
                                                (uint32_t)sz, buf);
    CETCD_ASSERT_NOT_NULL(raw);
    Etcd__KeyValue *kv2 = (Etcd__KeyValue *)raw;
    CETCD_ASSERT_EQ_INT((int)kv2->key_len, 6);
    CETCD_ASSERT_TRUE(memcmp(kv2->key, "my-key", 6) == 0);
    CETCD_ASSERT_EQ_INT((int)kv2->value_len, 8);
    CETCD_ASSERT_TRUE(memcmp(kv2->value, "my-value", 8) == 0);
    CETCD_ASSERT_EQ_INT((int)kv2->create_revision, 10);
    CETCD_ASSERT_EQ_INT((int)kv2->mod_revision, 20);
    CETCD_ASSERT_EQ_INT((int)kv2->version, 3);
    CETCD_ASSERT_EQ_INT((int)kv2->lease, 758300);

    cetcd_proto_free(raw);
    free(buf);
    free(kv);
}

CETCD_TEST_CASE(kv_message_default_values) {
    Etcd__KeyValue *kv = (Etcd__KeyValue *)calloc(1, sizeof(Etcd__KeyValue));
    kv->base.descriptor = &Etcd__KeyValue__descriptor;
    kv->key = (uint8_t *)"k";
    kv->key_len = 1;

    size_t sz = cetcd_proto_packed_size((ProtobufCMessage *)kv);
    uint8_t *buf = (uint8_t *)malloc(sz);
    size_t packed = cetcd_proto_pack((ProtobufCMessage *)kv, buf, sz);
    CETCD_ASSERT_TRUE(packed == sz);

    ProtobufCMessage *raw = cetcd_proto_unpack(&Etcd__KeyValue__descriptor,
                                                (uint32_t)sz, buf);
    CETCD_ASSERT_NOT_NULL(raw);
    Etcd__KeyValue *kv2 = (Etcd__KeyValue *)raw;
    CETCD_ASSERT_EQ_INT((int)kv2->key_len, 1);
    CETCD_ASSERT_EQ_INT((int)kv2->create_revision, 0);
    CETCD_ASSERT_EQ_INT((int)kv2->lease, 0);

    cetcd_proto_free(raw);
    free(buf);
    free(kv);
}

CETCD_TEST_CASE(kv_message_with_bytes) {
    uint8_t binary_key[] = {0x00, 0x01, 0x02, 0xff, 0xfe};
    uint8_t binary_val[] = {0xde, 0xad, 0xbe, 0xef};

    Etcd__KeyValue *kv = (Etcd__KeyValue *)calloc(1, sizeof(Etcd__KeyValue));
    kv->base.descriptor = &Etcd__KeyValue__descriptor;
    kv->key = binary_key;
    kv->key_len = 5;
    kv->value = binary_val;
    kv->value_len = 4;

    size_t sz = cetcd_proto_packed_size((ProtobufCMessage *)kv);
    uint8_t *buf = (uint8_t *)malloc(sz);
    cetcd_proto_pack((ProtobufCMessage *)kv, buf, sz);

    ProtobufCMessage *raw = cetcd_proto_unpack(&Etcd__KeyValue__descriptor,
                                                (uint32_t)sz, buf);
    CETCD_ASSERT_NOT_NULL(raw);
    Etcd__KeyValue *kv2 = (Etcd__KeyValue *)raw;
    CETCD_ASSERT_EQ_INT((int)kv2->key_len, 5);
    CETCD_ASSERT_TRUE(memcmp(kv2->key, binary_key, 5) == 0);
    CETCD_ASSERT_EQ_INT((int)kv2->value_len, 4);
    CETCD_ASSERT_TRUE(memcmp(kv2->value, binary_val, 4) == 0);

    cetcd_proto_free(raw);
    free(buf);
    free(kv);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(kv_message_pack_unpack_roundtrip),
    CETCD_TEST_ENTRY(kv_message_default_values),
    CETCD_TEST_ENTRY(kv_message_with_bytes),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
