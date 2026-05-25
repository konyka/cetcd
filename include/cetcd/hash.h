#ifndef CETCD_HASH_H_
#define CETCD_HASH_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint64_t cetcd_hash_fnv1a64(const void *data, size_t n);
uint64_t cetcd_hash_fnv1a64_seed(const void *data, size_t n, uint64_t seed);
uint32_t cetcd_crc32c(uint32_t crc, const void *data, size_t n);

#ifdef __cplusplus
}
#endif
#endif
