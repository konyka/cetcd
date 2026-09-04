#include "cetcd/wal.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    cetcd_entry e;
    memset(&e, 0, sizeof(e));
    (void)cetcd_wal_decode_entry(data, size, &e);
    free((void *)(uintptr_t)e.data.data);

    cetcd_hard_state hs;
    (void)cetcd_wal_decode_hard_state(data, size, &hs);

    uint64_t index = 0, term = 0;
    (void)cetcd_wal_decode_snapshot(data, size, &index, &term);
    return 0;
}

#include "fuzz_driver.h"
