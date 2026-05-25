#include "cetcd/base.h"

#define CETCD_FNV1A64_OFFSET 0xcbf29ce484222325ull
#define CETCD_FNV1A64_PRIME  0x100000001b3ull

uint64_t cetcd_hash_fnv1a64(const void *data, size_t n) {
    return cetcd_hash_fnv1a64_seed(data, n, CETCD_FNV1A64_OFFSET);
}

uint64_t cetcd_hash_fnv1a64_seed(const void *data, size_t n, uint64_t seed) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t h = seed ? seed : CETCD_FNV1A64_OFFSET;
    for (size_t i = 0; i < n; ++i) {
        h ^= (uint64_t)p[i];
        h *= CETCD_FNV1A64_PRIME;
    }
    return h;
}

/*
 * CRC32C (Castagnoli) — software fallback using the reflected-bit table method.
 * Polynomial 0x1EDC6F41 reflected = 0x82F63B78, exactly what etcd's WAL uses.
 */
static uint32_t crc32c_table[256];
static int      crc32c_table_inited = 0;

static void crc32c_init_table(void) {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
        }
        crc32c_table[i] = c;
    }
    crc32c_table_inited = 1;
}

uint32_t cetcd_crc32c(uint32_t crc, const void *data, size_t n) {
    if (!crc32c_table_inited) crc32c_init_table();
    const uint8_t *p = (const uint8_t *)data;
    uint32_t c = crc ^ 0xFFFFFFFFu;
    for (size_t i = 0; i < n; ++i) {
        c = crc32c_table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}
