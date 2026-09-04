#include "rpc.pb-c.h"
#include "kv.pb-c.h"
#include <stdlib.h>
#include <string.h>

/* Minimal descriptors (placeholders) */
const ProtobufCMessageDescriptor Etcd__RangeRequest__descriptor = {0};
const ProtobufCMessageDescriptor Etcd__RangeResponse__descriptor = {0};
const ProtobufCMessageDescriptor Etcd__PutRequest__descriptor = {0};
const ProtobufCMessageDescriptor Etcd__PutResponse__descriptor = {0};

/* We reuse Etcd__KeyValue__descriptor from kv.pb-c.c in tests; declare extern. */
extern const ProtobufCMessageDescriptor Etcd__KeyValue__descriptor;

/* Simple pack/unpack helpers for RangeRequest */
static size_t range_pack(const Etcd__RangeRequest *r, uint8_t *out) {
    size_t pos = 0;
    uint32_t kl = (uint32_t)r->key_len;
    if (out) { memcpy(out + pos, &kl, 4); }
    pos += 4;
    if (r->key_len && out) memcpy(out + pos, r->key, r->key_len);
    pos += r->key_len;
    uint32_t rl = (uint32_t)r->range_end_len;
    if (out) { memcpy(out + pos, &rl, 4); }
    pos += 4;
    if (r->range_end_len && out) memcpy(out + pos, r->range_end, r->range_end_len);
    pos += r->range_end_len;
    if (out) { memcpy(out + pos, &r->limit, 8); }
    pos += 8;
    if (out) { memcpy(out + pos, &r->revision, 8); }
    pos += 8;
    return pos;
}

static Etcd__RangeRequest *range_unpack(const uint8_t *data, size_t len) {
    if (!data) return NULL;
    Etcd__RangeRequest *r = (Etcd__RangeRequest *)calloc(1, sizeof(Etcd__RangeRequest));
    if (!r) return NULL;
    r->base.descriptor = &Etcd__RangeRequest__descriptor;
    size_t pos = 0;
    uint32_t kl = 0;
    if (len < 4) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
    memcpy(&kl, data + pos, 4);
    pos += 4;
    if (kl > len - pos) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
    r->key_len = kl;
    if (kl) {
        r->key = (uint8_t *)malloc(kl);
        if (!r->key) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
        memcpy(r->key, data + pos, kl);
        pos += kl;
    }
    if (pos + 4 > len) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
    uint32_t rl = 0;
    memcpy(&rl, data + pos, 4);
    pos += 4;
    if (rl > len - pos) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
    r->range_end_len = rl;
    if (rl) {
        r->range_end = (uint8_t *)malloc(rl);
        if (!r->range_end) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
        memcpy(r->range_end, data + pos, rl);
        pos += rl;
    }
    if (pos + 8 > len) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
    memcpy(&r->limit, data + pos, 8);
    pos += 8;
    if (pos + 8 > len) { cetcd_rpc_free((ProtobufCMessage *)r); return NULL; }
    memcpy(&r->revision, data + pos, 8);
    return r;
}

size_t cetcd_range_pack(const Etcd__RangeRequest *m, uint8_t *out, size_t out_len) {
    (void)out_len;
    return range_pack(m, out);
}

ProtobufCMessage *cetcd_range_unpack(const uint8_t *data, uint32_t len) {
    Etcd__RangeRequest *rv = range_unpack(data, len);
    if (!rv) return NULL;
    return (ProtobufCMessage*)rv;
}

/* Simple Put unpack/pack stubs */
static Etcd__PutRequest *put_unpack_impl(const uint8_t *data, size_t len) {
    if (!data) return NULL;
    Etcd__PutRequest *p = (Etcd__PutRequest*)calloc(1, sizeof(Etcd__PutRequest));
    p->base.descriptor = &Etcd__PutRequest__descriptor;
    /* Minimal decoding: first 4 bytes key_len, then key, then value_len, value, lease */
    size_t pos = 0; uint32_t kl=0; if (len < 4) { free(p); return NULL; } memcpy(&kl, data+pos, 4); pos+=4; if (kl && pos+kl <= len) { p->key = (uint8_t*)malloc(kl); memcpy(p->key, data+pos, kl); p->key_len = kl; pos+=kl; }
    uint32_t vl=0; if (pos+4 <= len) { memcpy(&vl, data+pos, 4); pos+=4; if (vl && pos+vl <= len) { p->value = (uint8_t*)malloc(vl); memcpy(p->value, data+pos, vl); p->value_len = vl; pos+=vl; } }
    if (pos + 8 <= len) { memcpy(&p->lease, data+pos, 8); pos += 8; }
    return p;
}

ProtobufCMessage *cetcd_put_unpack(const uint8_t *data, uint32_t len) {
    Etcd__PutRequest *p = put_unpack_impl(data, len);
    return (ProtobufCMessage*)p;
}

size_t cetcd_put_pack(const Etcd__PutRequest *m, uint8_t *out, size_t out_len) {
    (void)out_len;
    size_t pos = 0;
    uint32_t kl = (uint32_t)m->key_len;
    if (out) { memcpy(out + pos, &kl, 4); }
    pos += 4;
    if (kl && out) memcpy(out + pos, m->key, kl);
    pos += kl;
    uint32_t vl = (uint32_t)m->value_len;
    if (out) { memcpy(out + pos, &vl, 4); }
    pos += 4;
    if (vl && out) memcpy(out + pos, m->value, vl);
    pos += vl;
    if (out) { memcpy(out + pos, &m->lease, 8); }
    pos += 8;
    return pos;
}

void cetcd_rpc_free(ProtobufCMessage *msg) {
    if (!msg) return;
    Etcd__RangeRequest *r = (Etcd__RangeRequest*)msg;
    free(r->key); free(r->range_end); free(r);
}
