#include "cetcd/discovery.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    cetcd_srv_record recs[CETCD_DISCOVERY_MAX_RECORDS];
    size_t n = 0;
    (void)cetcd_discovery_parse_message(data, size, recs,
                                        CETCD_DISCOVERY_MAX_RECORDS, &n);

    char spec[512];
    size_t m = size < sizeof(spec) - 1 ? size : sizeof(spec) - 1;
    if (data && m > 0)
        memcpy(spec, data, m);
    spec[m] = '\0';
    cetcd_endpoint eps[CETCD_DISCOVERY_MAX_ENDPOINTS];
    size_t en = 0;
    (void)cetcd_endpoint_parse_list(spec, eps, CETCD_DISCOVERY_MAX_ENDPOINTS, &en);
    (void)cetcd_discovery_valid_domain(spec);
    (void)cetcd_discovery_valid_name(spec);
    char q[CETCD_DISCOVERY_MAX_QNAME];
    (void)cetcd_discovery_qname(CETCD_DISCOVERY_CLIENT, NULL, spec, q, sizeof(q));
    return 0;
}

#include "fuzz_driver.h"
