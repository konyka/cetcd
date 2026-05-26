#include "cetcd/raft.h"
#include <stdlib.h>
#include <string.h>

/* Raft log — stores entries in memory.
 * Will be expanded in Phase 3 when we add WAL persistence. */

typedef struct cetcd_raft_log_ {
    cetcd_entry *entries;
    uint32_t     count;
    uint32_t     capacity;
    uint64_t     offset;     /* first index in the log */
    uint64_t     committed;
    uint64_t     applied;
} cetcd_raft_log_;
