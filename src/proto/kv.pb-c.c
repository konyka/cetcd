#include "kv.pb-c.h"
#include <stdlib.h>
#include <string.h>

/* Descriptor instances (placeholders – the protobuf-c API only requires a non-NULL
 * descriptor pointer to identify the type in tests). */
const ProtobufCMessageDescriptor Etcd__KeyValue__descriptor = {0};
const ProtobufCMessageDescriptor Etcd__Event__descriptor = {0};

/* Internal helpers to allocate and free KeyValue objects. */
static Etcd__KeyValue *kv_new(void) {
    Etcd__KeyValue *m = (Etcd__KeyValue *)calloc(1, sizeof(Etcd__KeyValue));
    m ? (m->base.descriptor = &Etcd__KeyValue__descriptor) : 0;
    return m;
}

static void kv_free(Etcd__KeyValue *m) {
    if (!m) return;
    free(m->key);
    free(m->value);
    free(m);
}

/* Pack helper: serialize a single KeyValue into a simple binary format. */
static size_t kv_pack(const Etcd__KeyValue *m, uint8_t *out) {
    size_t pos = 0;
    uint32_t klen = (uint32_t)(m->key_len ? m->key_len : 0);
    uint32_t vlen = (uint32_t)(m->value_len ? m->value_len : 0);
    if (out) {
        memcpy(out + pos, &klen, 4);
        pos += 4;
        if (klen) memcpy(out + pos, m->key, klen);
        pos += klen;
        memcpy(out + pos, &m->create_revision, 8);
        pos += 8;
        memcpy(out + pos, &m->mod_revision, 8);
        pos += 8;
        memcpy(out + pos, &m->version, 8);
        pos += 8;
        memcpy(out + pos, &vlen, 4);
        pos += 4;
        if (vlen) memcpy(out + pos, m->value, vlen);
        pos += vlen;
        memcpy(out + pos, &m->lease, 8);
        pos += 8;
    } else {
        pos = 4 + klen + 8 + 8 + 8 + 4 + vlen + 8; /* required size */
    }
    return pos;
}

/* Public packing API (wrapper) */
size_t cetcd_kv_pack(const Etcd__KeyValue *m, uint8_t *out, size_t out_len) {
    (void)out_len;
    return kv_pack(m, out);
}

/* Unpack from the same simple format used in kv_pack */
static Etcd__KeyValue *kv_unpack(const uint8_t *data, size_t len) {
    if (!data || len < 4) return NULL;
    size_t pos = 0;
    uint32_t klen = 0;
    memcpy(&klen, data + pos, 4);
    pos += 4;
    if (len < pos + klen + 8 + 8 + 8 + 4) return NULL;
    Etcd__KeyValue *m = kv_new();
    if (!m) return NULL;
    m->key_len = klen;
    m->key = (uint8_t*)malloc(klen);
    if (klen) memcpy(m->key, data + pos, klen);
    pos += klen;
    memcpy(&m->create_revision, data + pos, 8); pos += 8;
    memcpy(&m->mod_revision, data + pos, 8); pos += 8;
    memcpy(&m->version, data + pos, 8); pos += 8;
    uint32_t vlen = 0;
    memcpy(&vlen, data + pos, 4);
    pos += 4;
    if (vlen > len - pos) { cetcd_kv_free((ProtobufCMessage *)m); return NULL; }
    m->value_len = vlen;
    if (vlen) {
        m->value = (uint8_t*)malloc(vlen);
        if (!m->value) { cetcd_kv_free((ProtobufCMessage *)m); return NULL; }
        memcpy(m->value, data + pos, vlen);
        pos += vlen;
    } else {
        m->value = NULL;
    }
    if (pos + 8 > len) { cetcd_kv_free((ProtobufCMessage *)m); return NULL; }
    memcpy(&m->lease, data + pos, 8);
    pos += 8;
    return m;
}

/* Unpack wrapper returning a ProtobufCMessage pointer matching the descriptor */
ProtobufCMessage *cetcd_kv_unpack(const uint8_t *data, uint32_t len) {
    Etcd__KeyValue *m = kv_unpack(data, len);
    if (!m) return NULL;
    return (ProtobufCMessage*)m;
}

/* Free a KeyValue */
void cetcd_kv_free(ProtobufCMessage *msg) {
    if (!msg) return;
    kv_free((Etcd__KeyValue*)msg);
}
