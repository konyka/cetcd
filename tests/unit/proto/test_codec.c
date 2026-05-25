#include "cetcd/base.h"
#include "cetcd/proto.h"
#include "cetcd_test.h"
#include "kv.pb-c.h"
#include <stdlib.h>

CETCD_TEST_CASE(codec_pack_returns_size) {
    Etcd__KeyValue kv;
    memset(&kv, 0, sizeof(kv));
    kv.base.descriptor = &Etcd__KeyValue__descriptor;
    kv.key = (uint8_t *)"a"; kv.key_len = 1;
    kv.value = (uint8_t *)"b"; kv.value_len = 1;
    kv.create_revision = 1; kv.mod_revision = 1; kv.version = 1; kv.lease = 0;
    size_t sz = cetcd_proto_packed_size((ProtobufCMessage *)&kv);
    CETCD_ASSERT_TRUE(sz > 0);
}

CETCD_TEST_CASE(codec_unpack_null_on_garbage) {
    ProtobufCMessage *m = cetcd_proto_unpack(&Etcd__KeyValue__descriptor,
                                              4, (const uint8_t *)"xxxx");
    CETCD_ASSERT_NULL(m);
}

CETCD_TEST_CASE(codec_free_null_safe) {
    cetcd_proto_free(NULL);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(codec_pack_returns_size),
    CETCD_TEST_ENTRY(codec_unpack_null_on_garbage),
    CETCD_TEST_ENTRY(codec_free_null_safe),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
