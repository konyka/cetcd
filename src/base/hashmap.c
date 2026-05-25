#include "cetcd/base.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef enum slot_state {
    SLOT_EMPTY     = 0,
    SLOT_OCCUPIED  = 1,
    SLOT_TOMBSTONE = 2
} slot_state;

typedef struct slot {
    uint64_t   hash;
    uint8_t   *key;
    size_t     keylen;
    void      *value;
    slot_state state;
} slot;

struct cetcd_hashmap {
    slot   *slots;
    size_t  cap;
    size_t  size;
    size_t  tombstones;
};

#define CETCD_HASHMAP_MIN_CAP        8u
#define CETCD_HASHMAP_LOAD_NUMER     7
#define CETCD_HASHMAP_LOAD_DENOM     10

static size_t next_pow2_(size_t n) {
    size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

static int map_alloc_table_(cetcd_hashmap *m, size_t cap) {
    m->slots = (slot *)calloc(cap, sizeof(slot));
    if (m->slots == NULL) return CETCD_ERR_NOMEM;
    m->cap = cap;
    m->size = 0;
    m->tombstones = 0;
    return 0;
}

cetcd_hashmap *cetcd_hashmap_new(size_t initial_cap) {
    cetcd_hashmap *m = (cetcd_hashmap *)calloc(1, sizeof(*m));
    if (m == NULL) return NULL;
    size_t cap = initial_cap < CETCD_HASHMAP_MIN_CAP
                 ? CETCD_HASHMAP_MIN_CAP : next_pow2_(initial_cap);
    if (map_alloc_table_(m, cap) != 0) { free(m); return NULL; }
    return m;
}

static void slot_clear_(slot *s) {
    if (s->state == SLOT_OCCUPIED) free(s->key);
    s->key = NULL;
    s->keylen = 0;
    s->value = NULL;
    s->hash = 0;
    s->state = SLOT_EMPTY;
}

void cetcd_hashmap_free(cetcd_hashmap *m) {
    if (m == NULL) return;
    for (size_t i = 0; i < m->cap; ++i) {
        if (m->slots[i].state == SLOT_OCCUPIED) free(m->slots[i].key);
    }
    free(m->slots);
    free(m);
}

size_t cetcd_hashmap_size(const cetcd_hashmap *m) {
    return m ? m->size : 0;
}

size_t cetcd_hashmap_capacity(const cetcd_hashmap *m) {
    return m ? m->cap : 0;
}

static bool slot_key_equal_(const slot *s, cetcd_slice key) {
    return s->keylen == key.len && memcmp(s->key, key.data, key.len) == 0;
}

static size_t find_slot_(const cetcd_hashmap *m, cetcd_slice key, uint64_t h, bool *found_out) {
    size_t mask = m->cap - 1;
    size_t idx  = (size_t)(h & mask);
    size_t first_tomb = (size_t)-1;
    for (size_t probe = 0; probe < m->cap; ++probe) {
        size_t i = (idx + probe) & mask;
        const slot *s = &m->slots[i];
        if (s->state == SLOT_EMPTY) {
            if (found_out) *found_out = false;
            return first_tomb != (size_t)-1 ? first_tomb : i;
        }
        if (s->state == SLOT_TOMBSTONE) {
            if (first_tomb == (size_t)-1) first_tomb = i;
            continue;
        }
        if (s->hash == h && slot_key_equal_(s, key)) {
            if (found_out) *found_out = true;
            return i;
        }
    }
    if (found_out) *found_out = false;
    return first_tomb != (size_t)-1 ? first_tomb : 0;
}

static int map_resize_(cetcd_hashmap *m, size_t new_cap) {
    slot   *old_slots = m->slots;
    size_t  old_cap   = m->cap;

    if (map_alloc_table_(m, new_cap) != 0) {
        m->slots = old_slots;
        m->cap   = old_cap;
        return CETCD_ERR_NOMEM;
    }

    for (size_t i = 0; i < old_cap; ++i) {
        slot *s = &old_slots[i];
        if (s->state != SLOT_OCCUPIED) continue;
        size_t mask = m->cap - 1;
        size_t idx  = (size_t)(s->hash & mask);
        for (size_t probe = 0; probe < m->cap; ++probe) {
            size_t j = (idx + probe) & mask;
            if (m->slots[j].state == SLOT_EMPTY) {
                m->slots[j] = *s;
                ++m->size;
                break;
            }
        }
    }

    free(old_slots);
    return 0;
}

int cetcd_hashmap_put(cetcd_hashmap *m, cetcd_slice key, void *value) {
    if (m == NULL) return CETCD_ERR_INVAL;

    if ((m->size + m->tombstones + 1) * CETCD_HASHMAP_LOAD_DENOM
        > m->cap * CETCD_HASHMAP_LOAD_NUMER) {
        size_t new_cap = m->cap * 2;
        if (new_cap < CETCD_HASHMAP_MIN_CAP) new_cap = CETCD_HASHMAP_MIN_CAP;
        int rc = map_resize_(m, new_cap);
        if (rc != 0) return rc;
    }

    uint64_t h = cetcd_hash_fnv1a64(key.data, key.len);
    bool found = false;
    size_t idx = find_slot_(m, key, h, &found);
    slot *s = &m->slots[idx];

    if (found) {
        s->value = value;
        return 0;
    }

    uint8_t *kdup = NULL;
    if (key.len > 0) {
        kdup = (uint8_t *)malloc(key.len);
        if (kdup == NULL) return CETCD_ERR_NOMEM;
        memcpy(kdup, key.data, key.len);
    }

    if (s->state == SLOT_TOMBSTONE) {
        if (m->tombstones > 0) --m->tombstones;
    }
    s->hash   = h;
    s->key    = kdup;
    s->keylen = key.len;
    s->value  = value;
    s->state  = SLOT_OCCUPIED;
    ++m->size;
    return 0;
}

bool cetcd_hashmap_get(const cetcd_hashmap *m, cetcd_slice key, void **value_out) {
    if (m == NULL || m->size == 0) return false;
    uint64_t h = cetcd_hash_fnv1a64(key.data, key.len);
    bool found = false;
    size_t idx = find_slot_(m, key, h, &found);
    if (!found) return false;
    if (value_out) *value_out = m->slots[idx].value;
    return true;
}

bool cetcd_hashmap_contains(const cetcd_hashmap *m, cetcd_slice key) {
    return cetcd_hashmap_get(m, key, NULL);
}

bool cetcd_hashmap_remove(cetcd_hashmap *m, cetcd_slice key, void **value_out) {
    if (m == NULL || m->size == 0) return false;
    uint64_t h = cetcd_hash_fnv1a64(key.data, key.len);
    bool found = false;
    size_t idx = find_slot_(m, key, h, &found);
    if (!found) return false;
    slot *s = &m->slots[idx];
    if (value_out) *value_out = s->value;
    free(s->key);
    s->key    = NULL;
    s->keylen = 0;
    s->value  = NULL;
    s->state  = SLOT_TOMBSTONE;
    --m->size;
    ++m->tombstones;
    return true;
}

void cetcd_hashmap_iter(const cetcd_hashmap *m,
                        cetcd_hashmap_iter_fn fn, void *udata) {
    if (m == NULL || fn == NULL) return;
    for (size_t i = 0; i < m->cap; ++i) {
        const slot *s = &m->slots[i];
        if (s->state != SLOT_OCCUPIED) continue;
        cetcd_slice key;
        key.data = s->key;
        key.len  = s->keylen;
        if (!fn(key, s->value, udata)) return;
    }
}
