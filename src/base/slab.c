#include "cetcd/base.h"
#include "cetcd/buf.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define CETCD_SLAB_DEFAULT_PER_BLOCK 64u

/* ── Global slab registry for profiling ───────────────────────────────── */

typedef struct slab_registry_node_ {
    cetcd_slab             *slab;
    struct slab_registry_node_ *next;
} slab_registry_node_;

static slab_registry_node_ *g_slab_registry = NULL;

static void slab_registry_add_(cetcd_slab *s) {
    slab_registry_node_ *node = (slab_registry_node_ *)malloc(sizeof(*node));
    if (!node) return;
    node->slab = s;
    node->next = g_slab_registry;
    g_slab_registry = node;
}

static void slab_registry_remove_(cetcd_slab *s) {
    slab_registry_node_ **pp = &g_slab_registry;
    while (*pp) {
        if ((*pp)->slab == s) {
            slab_registry_node_ *del = *pp;
            *pp = del->next;
            free(del);
            return;
        }
        pp = &(*pp)->next;
    }
}

void cetcd_slab_walk(cetcd_slab_walk_fn fn, void *ud) {
    if (!fn) return;
    for (slab_registry_node_ *n = g_slab_registry; n; n = n->next) {
        fn(n->slab, ud);
    }
}

typedef struct slab_block {
    struct slab_block *next;
    uint8_t           *base;
    size_t             count;
} slab_block;

typedef struct slab_free_node {
    struct slab_free_node *next;
} slab_free_node;

struct cetcd_slab {
    size_t          obj_size;
    size_t          objs_per_block;
    slab_block     *blocks;
    slab_free_node *free_list;
    size_t          live;
    size_t          block_count;
};

static size_t align_up_(size_t n, size_t a) {
    return (n + (a - 1)) & ~(a - 1);
}

cetcd_slab *cetcd_slab_new(size_t obj_size, size_t objs_per_block) {
    if (obj_size == 0) return NULL;
    if (obj_size < sizeof(slab_free_node)) obj_size = sizeof(slab_free_node);
    obj_size = align_up_(obj_size, sizeof(void *));

    if (objs_per_block == 0) objs_per_block = CETCD_SLAB_DEFAULT_PER_BLOCK;

    cetcd_slab *s = (cetcd_slab *)calloc(1, sizeof(*s));
    if (s == NULL) return NULL;
    s->obj_size       = obj_size;
    s->objs_per_block = objs_per_block;
    slab_registry_add_(s);
    return s;
}

void cetcd_slab_free(cetcd_slab *s) {
    if (s == NULL) return;
    slab_registry_remove_(s);
    slab_block *b = s->blocks;
    while (b) {
        slab_block *next = b->next;
        free(b->base);
        free(b);
        b = next;
    }
    free(s);
}

static int slab_grow_(cetcd_slab *s) {
    slab_block *b = (slab_block *)malloc(sizeof(*b));
    if (b == NULL) return CETCD_ERR_NOMEM;
    b->base = (uint8_t *)malloc(s->obj_size * s->objs_per_block);
    if (b->base == NULL) { free(b); return CETCD_ERR_NOMEM; }
    b->count = s->objs_per_block;
    b->next  = s->blocks;
    s->blocks = b;
    s->block_count += 1;
    for (size_t i = 0; i < b->count; ++i) {
        slab_free_node *node = (slab_free_node *)(b->base + i * s->obj_size);
        node->next = s->free_list;
        s->free_list = node;
    }
    return 0;
}

void *cetcd_slab_alloc(cetcd_slab *s) {
    if (s == NULL) return NULL;
    if (s->free_list == NULL) {
        if (slab_grow_(s) != 0) return NULL;
    }
    slab_free_node *node = s->free_list;
    s->free_list = node->next;
    s->live += 1;
    return node;
}

void cetcd_slab_release(cetcd_slab *s, void *obj) {
    if (s == NULL || obj == NULL) return;
    slab_free_node *node = (slab_free_node *)obj;
    node->next = s->free_list;
    s->free_list = node;
    if (s->live > 0) s->live -= 1;
}

size_t cetcd_slab_obj_size(const cetcd_slab *s) { return s ? s->obj_size : 0; }
size_t cetcd_slab_live_count(const cetcd_slab *s) { return s ? s->live : 0; }
size_t cetcd_slab_block_count(const cetcd_slab *s) { return s ? s->block_count : 0; }

void cetcd_slab_get_stats(const cetcd_slab *s, cetcd_slab_stats *out) {
    if (!s || !out) return;
    memset(out, 0, sizeof(*out));
    out->obj_size       = s->obj_size;
    out->objs_per_block = s->objs_per_block;
    out->block_count    = s->block_count;
    out->live_count     = s->live;
    out->total_capacity = s->block_count * s->objs_per_block;
}
