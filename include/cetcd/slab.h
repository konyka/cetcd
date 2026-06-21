#ifndef CETCD_SLAB_H_
#define CETCD_SLAB_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_slab cetcd_slab;

cetcd_slab *cetcd_slab_new(size_t obj_size, size_t objs_per_block);
void        cetcd_slab_free(cetcd_slab *s);
void       *cetcd_slab_alloc(cetcd_slab *s);
void        cetcd_slab_release(cetcd_slab *s, void *obj);
size_t      cetcd_slab_obj_size(const cetcd_slab *s);
size_t      cetcd_slab_live_count(const cetcd_slab *s);
size_t      cetcd_slab_block_count(const cetcd_slab *s);

/* Slab registry for profiling — walk all live slab allocators.
 * The callback receives each slab and the user-data pointer. */
typedef void (*cetcd_slab_walk_fn)(const cetcd_slab *s, void *ud);
void cetcd_slab_walk(cetcd_slab_walk_fn fn, void *ud);

/* Per-slab statistics for profiling. */
typedef struct cetcd_slab_stats {
    size_t obj_size;
    size_t objs_per_block;
    size_t block_count;
    size_t live_count;
    size_t total_capacity;   /* block_count * objs_per_block */
} cetcd_slab_stats;

void cetcd_slab_get_stats(const cetcd_slab *s, cetcd_slab_stats *out);

#ifdef __cplusplus
}
#endif
#endif
