#ifndef CETCD_ARENA_H_
#define CETCD_ARENA_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_arena cetcd_arena;

cetcd_arena *cetcd_arena_new(size_t block_size);
void         cetcd_arena_free(cetcd_arena *a);
void        *cetcd_arena_alloc(cetcd_arena *a, size_t n);
void        *cetcd_arena_alloc_aligned(cetcd_arena *a, size_t n, size_t align);
char        *cetcd_arena_strdup(cetcd_arena *a, const char *s);
void        *cetcd_arena_memdup(cetcd_arena *a, const void *p, size_t n);
void         cetcd_arena_reset(cetcd_arena *a);
size_t       cetcd_arena_total_allocated(const cetcd_arena *a);
size_t       cetcd_arena_total_bytes_used(const cetcd_arena *a);
size_t       cetcd_arena_block_count(const cetcd_arena *a);

#ifdef __cplusplus
}
#endif
#endif
