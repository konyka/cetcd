#ifndef CETCD_DISCOVERY_H_
#define CETCD_DISCOVERY_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CETCD_DISCOVERY_MAX_RECORDS 32
#define CETCD_DISCOVERY_MAX_ENDPOINTS 32
#define CETCD_DISCOVERY_MAX_QNAME 512

typedef enum cetcd_discovery_kind {
    CETCD_DISCOVERY_CLIENT     = 0,
    CETCD_DISCOVERY_CLIENT_SSL = 1,
    CETCD_DISCOVERY_SERVER     = 2,
    CETCD_DISCOVERY_SERVER_SSL = 3
} cetcd_discovery_kind;

typedef struct cetcd_srv_record {
    uint16_t priority;
    uint16_t weight;
    uint16_t port;
    char     target[256];
} cetcd_srv_record;

typedef struct cetcd_endpoint {
    char     host[256];
    uint16_t port;
    int      https; /* 1 = https:// */
} cetcd_endpoint;

/* DNS name: 1..253, labels 1..63, [A-Za-z0-9-], no empty/leading/trailing
 * hyphen on a label. A trailing dot is stripped. Returns 0 if valid. */
int cetcd_discovery_valid_domain(const char *domain);

/* SRV name suffix: empty (ok) or 1..63 [A-Za-z0-9-]. Returns 0 if valid. */
int cetcd_discovery_valid_name(const char *name);

/* Build `_<service>[-<name>]._tcp.<domain>` into out. Fail-closed. */
int cetcd_discovery_qname(cetcd_discovery_kind kind, const char *name,
                          const char *domain, char *out, size_t cap);

/* Parse a DNS message for SRV answers. Pointer loops, truncated RDATA,
 * port 0, and empty targets are rejected. *n is set on success. */
int cetcd_discovery_parse_message(const uint8_t *msg, size_t len,
                                  cetcd_srv_record *out, size_t cap, size_t *n);

/* Deterministic order: priority asc, weight desc, target strcmp.
 * Used so failover is repeatable (no RNG on the connect path). */
void cetcd_discovery_sort(cetcd_srv_record *recs, size_t n);

/* OS DNS lookup (res_query / DnsQuery). Fail-closed on 0 records. */
int cetcd_discovery_lookup(const char *qname,
                           cetcd_srv_record *out, size_t cap, size_t *n);

/* Copy SRV targets into endpoints. https is applied to every record.
 * More records than cap is fail-closed (nothing is silently dropped). */
int cetcd_discovery_records_to_endpoints(const cetcd_srv_record *recs, size_t n,
                                         int https, cetcd_endpoint *out,
                                         size_t cap, size_t *nout);

/* qname + lookup + sort + to_endpoints. */
int cetcd_discovery_resolve(cetcd_discovery_kind kind, const char *name,
                            const char *domain, int https,
                            cetcd_endpoint *out, size_t cap, size_t *n);

/* Stable Raft id from target:port (FNV-1a). Never 0. */
int cetcd_discovery_peer_id(const char *target, uint16_t port, uint64_t *id);

/* Parse `host:port`, `http(s)://host:port`, `[v6]:port`, comma lists.
 * Default port is 2379. Leftover text, port 0, empty host, or more
 * endpoints than cap fail-closed. */
int cetcd_endpoint_parse_list(const char *spec, cetcd_endpoint *out,
                              size_t cap, size_t *n);

#ifdef __cplusplus
}
#endif
#endif /* CETCD_DISCOVERY_H_ */
