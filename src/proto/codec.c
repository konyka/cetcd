#include "cetcd/proto.h"
#include "kv.pb-c.h"
#include "rpc.pb-c.h"
#include <stdlib.h>
#include <string.h>

size_t cetcd_proto_pack(const ProtobufCMessage *msg, uint8_t *out, size_t out_len) {
    if (!msg || !msg->descriptor) return 0;
    if (msg->descriptor == &Etcd__KeyValue__descriptor) {
        return cetcd_kv_pack((const Etcd__KeyValue *)msg, out, out_len);
    }
    if (msg->descriptor == &Etcd__RangeRequest__descriptor) {
        return cetcd_range_pack((const Etcd__RangeRequest *)msg, out, out_len);
    }
    if (msg->descriptor == &Etcd__PutRequest__descriptor) {
        return cetcd_put_pack((const Etcd__PutRequest *)msg, out, out_len);
    }
    return 0;
}

ProtobufCMessage *cetcd_proto_unpack(const ProtobufCMessageDescriptor *desc,
                                      uint32_t len, const uint8_t *data) {
    if (!desc) return NULL;
    if (desc == &Etcd__KeyValue__descriptor) {
        return cetcd_kv_unpack(data, len);
    }
    if (desc == &Etcd__RangeRequest__descriptor) {
        return cetcd_range_unpack(data, len);
    }
    if (desc == &Etcd__PutRequest__descriptor) {
        return cetcd_put_unpack(data, len);
    }
    return NULL;
}

void cetcd_proto_free(ProtobufCMessage *msg) {
    if (!msg) return;
    if (msg->descriptor == &Etcd__KeyValue__descriptor) {
        cetcd_kv_free(msg);
        return;
    }
    cetcd_rpc_free(msg);
}

size_t cetcd_proto_packed_size(const ProtobufCMessage *msg) {
    if (!msg || !msg->descriptor) return 0;
    if (msg->descriptor == &Etcd__KeyValue__descriptor) {
        return cetcd_kv_pack((const Etcd__KeyValue *)msg, NULL, 0);
    }
    if (msg->descriptor == &Etcd__RangeRequest__descriptor) {
        return cetcd_range_pack((const Etcd__RangeRequest *)msg, NULL, 0);
    }
    if (msg->descriptor == &Etcd__PutRequest__descriptor) {
        return cetcd_put_pack((const Etcd__PutRequest *)msg, NULL, 0);
    }
    return 0;
}
