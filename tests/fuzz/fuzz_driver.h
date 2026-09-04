#ifndef CETCD_FUZZ_DRIVER_H_
#define CETCD_FUZZ_DRIVER_H_

#ifdef CETCD_FUZZ_DRIVER
#include <stddef.h>
#include <stdint.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int main(void) {
    static const uint8_t empty = 0;
    (void)LLVMFuzzerTestOneInput(&empty, 0);
    (void)LLVMFuzzerTestOneInput(&empty, 1);
    uint8_t junk[128];
    for (size_t i = 0; i < sizeof(junk); i++)
        junk[i] = (uint8_t)(i * 37u + 11u);
    (void)LLVMFuzzerTestOneInput(junk, sizeof(junk));
    return 0;
}
#endif

#endif
