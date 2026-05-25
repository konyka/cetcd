#ifndef CETCD_PROTO_H
#define CETCD_PROTO_H

#include "cetcd/base.h"
#include "protobuf-c.h"

/*
 * Public umbrella header for CETCD's protobuf-like messages.
 * We provide a very small subset of the protobuf-c based API required by the
 * unit tests in Phase 0. The actual message structs are implemented in
 * src/proto/ as handwritten pb-c compatible structures.
 */

/* Forward declarations for the message types we implement. */
typedef struct Etcd__KeyValue            Etcd__KeyValue;
typedef struct Etcd__Event               Etcd__Event;
typedef struct Etcd__RangeRequest        Etcd__RangeRequest;
typedef struct Etcd__RangeResponse       Etcd__RangeResponse;
typedef struct Etcd__PutRequest          Etcd__PutRequest;
typedef struct Etcd__PutResponse         Etcd__PutResponse;
typedef struct Etcd__RequestOp           Etcd__RequestOp;
typedef struct Etcd__ResponseOp          Etcd__ResponseOp;

/* Descriptors – defined in the corresponding .c files. Expose them here so the
 * tests can reference the exact type during pack/unpack. */
extern const ProtobufCMessageDescriptor Etcd__KeyValue__descriptor;
extern const ProtobufCMessageDescriptor Etcd__Event__descriptor;
extern const ProtobufCMessageDescriptor Etcd__RangeRequest__descriptor;
extern const ProtobufCMessageDescriptor Etcd__RangeResponse__descriptor;
extern const ProtobufCMessageDescriptor Etcd__PutRequest__descriptor;
extern const ProtobufCMessageDescriptor Etcd__PutResponse__descriptor;
extern const ProtobufCMessageDescriptor Etcd__RequestOp__descriptor;
extern const ProtobufCMessageDescriptor Etcd__ResponseOp__descriptor;

/* Allocation helpers wrap protobuf-c when available, but tests rely on a
 * very small API surface. */
size_t cetcd_proto_pack(const ProtobufCMessage *msg, uint8_t *out, size_t out_len);
ProtobufCMessage *cetcd_proto_unpack(const ProtobufCMessageDescriptor *desc,
                                     uint32_t len, const uint8_t *data);
void cetcd_proto_free(ProtobufCMessage *msg);
size_t cetcd_proto_packed_size(const ProtobufCMessage *msg);

#endif /* CETCD_PROTO_H */
