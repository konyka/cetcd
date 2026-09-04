#include "cetcd/proto.h"

#include <stddef.h>
#include <stdint.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    uint32_t len = size > UINT32_MAX ? UINT32_MAX : (uint32_t)size;
    ProtobufCMessage *m = cetcd_proto_unpack(&Etcd__RangeRequest__descriptor, len, data);
    cetcd_proto_free(m);
    m = cetcd_proto_unpack(&Etcd__PutRequest__descriptor, len, data);
    cetcd_proto_free(m);
    m = cetcd_proto_unpack(&Etcd__KeyValue__descriptor, len, data);
    cetcd_proto_free(m);
    return 0;
}

#include "fuzz_driver.h"
