#ifndef CETCD_HASHMAP_H_
#define CETCD_HASHMAP_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "cetcd/slice.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cetcd_hashmap cetcd_hashmap;

cetcd_hashmap *cetcd_hashmap_new(size_t initial_cap);
void           cetcd_hashmap_free(cetcd_hashmap *m);
size_t         cetcd_hashmap_size(const cetcd_hashmap *m);
size_t         cetcd_hashmap_capacity(const cetcd_hashmap *m);

int            cetcd_hashmap_put(cetcd_hashmap *m, cetcd_slice key, void *value);
bool           cetcd_hashmap_get(const cetcd_hashmap *m, cetcd_slice key, void **value_out);
bool           cetcd_hashmap_remove(cetcd_hashmap *m, cetcd_slice key, void **value_out);
bool           cetcd_hashmap_contains(const cetcd_hashmap *m, cetcd_slice key);

typedef bool (*cetcd_hashmap_iter_fn)(cetcd_slice key, void *value, void *udata);
void           cetcd_hashmap_iter(const cetcd_hashmap *m,
                                  cetcd_hashmap_iter_fn fn, void *udata);

#ifdef __cplusplus
}
#endif
#endif
