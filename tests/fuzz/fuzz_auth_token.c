#include "cetcd/auth.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    char spec[CETCD_AUTH_MAX_TOKEN_LEN + 1];
    size_t n = size < CETCD_AUTH_MAX_TOKEN_LEN ? size : CETCD_AUTH_MAX_TOKEN_LEN;
    if (data && n > 0)
        memcpy(spec, data, n);
    spec[n] = '\0';

    cetcd_auth_store *s = cetcd_auth_store_new();
    if (!s) return 0;
    (void)cetcd_auth_set_token_spec(s, spec);
    (void)cetcd_auth_user_for_token(s, spec, 0);
    cetcd_auth_store_free(s);
    return 0;
}

#include "fuzz_driver.h"
