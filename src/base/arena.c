#include "cetcd/base.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define CETCD_ARENA_DEFAULT_BLOCK 4096u
#define CETCD_ARENA_MAX_INLINE    2048u

typedef struct arena_block {
    struct arena_block *next;
    size_t              cap;
    size_t              used;
    uint8_t            *base;
} arena_block;

struct cetcd_arena {
    arena_block *head;
    arena_block *huge;
    size_t       block_size;
    size_t       total_allocated;
    size_t       total_used;
    size_t       block_count;
};

static arena_block *arena_block_new_(size_t cap) {
    arena_block *blk = (arena_block *)malloc(sizeof(arena_block) + cap);
    if (blk == NULL) return NULL;
    blk->next = NULL;
    blk->cap  = cap;
    blk->used = 0;
    blk->base = (uint8_t *)(blk + 1);
    return blk;
}

cetcd_arena *cetcd_arena_new(size_t block_size) {
    if (block_size == 0) block_size = CETCD_ARENA_DEFAULT_BLOCK;
    cetcd_arena *a = (cetcd_arena *)calloc(1, sizeof(*a));
    if (a == NULL) return NULL;
    a->block_size = block_size;
    return a;
}

void cetcd_arena_free(cetcd_arena *a) {
    if (a == NULL) return;
    arena_block *b = a->head;
    while (b) {
        arena_block *next = b->next;
        free(b);
        b = next;
    }
    b = a->huge;
    while (b) {
        arena_block *next = b->next;
        free(b);
        b = next;
    }
    free(a);
}

void *cetcd_arena_alloc_aligned(cetcd_arena *a, size_t n, size_t align) {
    if (a == NULL) return NULL;
    if (align == 0) align = 1;
    /* Power-of-two enforcement: round up to next power of two if needed. */
    size_t real_align = 1;
    while (real_align < align) real_align <<= 1;

    if (n == 0) n = 1;

    if (n > CETCD_ARENA_MAX_INLINE || n > a->block_size) {
        arena_block *blk = arena_block_new_(n + real_align);
        if (blk == NULL) return NULL;
        uintptr_t base = (uintptr_t)blk->base;
        uintptr_t aligned = (base + (real_align - 1)) & ~(uintptr_t)(real_align - 1);
        blk->used = (size_t)(aligned - base) + n;
        blk->next = a->huge;
        a->huge   = blk;
        a->total_allocated += blk->cap;
        a->total_used      += blk->used;
        a->block_count     += 1;
        return (void *)aligned;
    }

    if (a->head == NULL) {
        a->head = arena_block_new_(a->block_size);
        if (a->head == NULL) return NULL;
        a->total_allocated += a->head->cap;
        a->block_count     += 1;
    }

    arena_block *b = a->head;
    uintptr_t base = (uintptr_t)(b->base + b->used);
    uintptr_t aligned = (base + (real_align - 1)) & ~(uintptr_t)(real_align - 1);
    size_t padding = (size_t)(aligned - base);

    if (b->used + padding + n > b->cap) {
        size_t cap = a->block_size;
        if (cap < n + real_align) cap = n + real_align;
        arena_block *nb = arena_block_new_(cap);
        if (nb == NULL) return NULL;
        nb->next = a->head;
        a->head  = nb;
        a->total_allocated += nb->cap;
        a->block_count     += 1;

        base    = (uintptr_t)nb->base;
        aligned = (base + (real_align - 1)) & ~(uintptr_t)(real_align - 1);
        padding = (size_t)(aligned - base);
        b = nb;
    }

    b->used        += padding + n;
    a->total_used  += padding + n;
    return (void *)aligned;
}

void *cetcd_arena_alloc(cetcd_arena *a, size_t n) {
    return cetcd_arena_alloc_aligned(a, n, sizeof(void *));
}

char *cetcd_arena_strdup(cetcd_arena *a, const char *s) {
    if (s == NULL) return NULL;
    size_t n = strlen(s) + 1;
    char *p = (char *)cetcd_arena_alloc_aligned(a, n, 1);
    if (p == NULL) return NULL;
    memcpy(p, s, n);
    return p;
}

void *cetcd_arena_memdup(cetcd_arena *a, const void *p, size_t n) {
    if (p == NULL || n == 0) return NULL;
    void *dst = cetcd_arena_alloc_aligned(a, n, 1);
    if (dst == NULL) return NULL;
    memcpy(dst, p, n);
    return dst;
}

void cetcd_arena_reset(cetcd_arena *a) {
    if (a == NULL) return;
    arena_block *first = a->head;
    if (first != NULL) {
        arena_block *b = first->next;
        while (b) {
            arena_block *next = b->next;
            if (a->total_allocated >= b->cap) a->total_allocated -= b->cap;
            if (a->block_count > 0)           a->block_count     -= 1;
            free(b);
            b = next;
        }
        first->next = NULL;
        first->used = 0;
    }
    arena_block *h = a->huge;
    while (h) {
        arena_block *next = h->next;
        if (a->total_allocated >= h->cap) a->total_allocated -= h->cap;
        if (a->block_count > 0)           a->block_count     -= 1;
        free(h);
        h = next;
    }
    a->huge       = NULL;
    a->total_used = 0;
}

size_t cetcd_arena_total_allocated(const cetcd_arena *a) {
    return a ? a->total_allocated : 0;
}

size_t cetcd_arena_total_bytes_used(const cetcd_arena *a) {
    return a ? a->total_used : 0;
}

size_t cetcd_arena_block_count(const cetcd_arena *a) {
    return a ? a->block_count : 0;
}
