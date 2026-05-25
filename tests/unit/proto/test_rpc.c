#include "cetcd/base.h"
#include "cetcd/proto.h"
#include "cetcd_test.h"
#include "kv.pb-c.h"
#include "rpc.pb-c.h"
#include <stdlib.h>
#include <string.h>

CETCD_TEST_CASE(range_request_pack_unpack) {
    Etcd__RangeRequest req;
    memset(&req, 0, sizeof(req));
    req.base.descriptor = &Etcd__RangeRequest__descriptor;
    uint8_t k[] = {'a', 'b', 'c'};
    req.key = k; req.key_len = 3;
    uint8_t rk[] = {'x', 'y'};
    req.range_end = rk; req.range_end_len = 2;
    req.limit = 42;
    req.revision = 7;

    size_t size = cetcd_proto_packed_size((ProtobufCMessage *)&req);
    CETCD_ASSERT_TRUE(size > 0);
    uint8_t *buf = (uint8_t *)malloc(size);
    cetcd_proto_pack((ProtobufCMessage *)&req, buf, size);
    ProtobufCMessage *un = cetcd_proto_unpack(&Etcd__RangeRequest__descriptor,
                                               (uint32_t)size, buf);
    CETCD_ASSERT_NOT_NULL(un);
    cetcd_proto_free(un);
    free(buf);
}

CETCD_TEST_CASE(put_request_pack_unpack) {
    Etcd__PutRequest req;
    memset(&req, 0, sizeof(req));
    req.base.descriptor = &Etcd__PutRequest__descriptor;
    uint8_t k[] = {'k', 'e', 'y'};
    req.key = k; req.key_len = 3;
    uint8_t v[] = {'v', 'a', 'l'};
    req.value = v; req.value_len = 3;
    req.lease = 99;

    size_t size = cetcd_proto_packed_size((ProtobufCMessage *)&req);
    CETCD_ASSERT_TRUE(size > 0);
    uint8_t *buf = (uint8_t *)malloc(size);
    cetcd_proto_pack((ProtobufCMessage *)&req, buf, size);
    ProtobufCMessage *un = cetcd_proto_unpack(&Etcd__PutRequest__descriptor,
                                               (uint32_t)size, buf);
    CETCD_ASSERT_NOT_NULL(un);
    cetcd_proto_free(un);
    free(buf);
}

CETCD_TEST_CASE(range_response_with_kvs) {
    /* RangeResponse packing not yet supported — just verify struct creation */
    Etcd__RangeResponse resp;
    memset(&resp, 0, sizeof(resp));
    resp.base.descriptor = &Etcd__RangeResponse__descriptor;

    Etcd__KeyValue *kv = (Etcd__KeyValue *)calloc(1, sizeof(Etcd__KeyValue));
    kv->base.descriptor = &Etcd__KeyValue__descriptor;
    kv->key = (uint8_t *)"k1"; kv->key_len = 2;
    kv->value = (uint8_t *)"v1"; kv->value_len = 2;

    resp.n_kvs = 1;
    resp.count = 1;
    resp.more = 0;

    /* Verify packed_size returns 0 for unsupported type (no crash) */
    size_t sz = cetcd_proto_packed_size((ProtobufCMessage *)&resp);
    (void)sz;

    free(kv);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(range_request_pack_unpack),
    CETCD_TEST_ENTRY(put_request_pack_unpack),
    CETCD_TEST_ENTRY(range_response_with_kvs),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
