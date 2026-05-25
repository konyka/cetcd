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

#ifdef __cplusplus
}
#endif
#endif
