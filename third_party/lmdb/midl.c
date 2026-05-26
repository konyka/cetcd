#include "midl.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

MDB_IDL mdb_midl_alloc(int num) {
    MDB_IDL ids = (MDB_IDL)malloc((num + 2) * sizeof(MDB_ID));
    if (ids) { *ids++ = num; *ids = 0; }
    return ids;
}

void mdb_midl_free(MDB_IDL ids) {
    if (ids) free(ids - 1);
}

void mdb_midl_shrink(MDB_IDL *idp) {
    (void)idp;
}

int mdb_midl_need(MDB_IDL *idp, unsigned num) {
    (void)idp; (void)num;
    return 0;
}

int mdb_midl_append(MDB_IDL *idp, MDB_ID id) {
    MDB_IDL ids = *idp;
    if (ids[0] >= ids[-1]) {
        int x = (int)(ids[-1] << 1);
        MDB_IDL idn = (MDB_IDL)realloc(ids - 1, (x + 2) * sizeof(MDB_ID));
        if (!idn) return ENOMEM;
        idn[0] = x;
        ids = idn + 1;
        *idp = ids;
    }
    ids[0]++;
    ids[ids[0]] = id;
    return 0;
}

int mdb_midl_append_ids(MDB_IDL *idp, MDB_ID *ids2, int n) {
    (void)idp; (void)ids2; (void)n;
    return 0;
}

static int mdb_midl_cmp(const void *a, const void *b) {
    return (*(MDB_ID *)a < *(MDB_ID *)b) ? -1 : (*(MDB_ID *)a > *(MDB_ID *)b);
}

void mdb_midl_sort(MDB_IDL ids) {
    if (ids[0] > 1)
        qsort(ids + 1, (size_t)ids[0], sizeof(MDB_ID), mdb_midl_cmp);
}

void mdb_midl_xmerge(MDB_IDL idl, MDB_IDL merge) {
    (void)idl; (void)merge;
}

unsigned mdb_mid2l_search(MDB_ID2L ids, MDB_ID id) {
    unsigned base = 0, cursor = 1, n = (unsigned)ids[0].mid;
    while (n > 0) {
        unsigned pivot = n >> 1;
        cursor = base + pivot + 1;
        if (ids[cursor].mid < id) { base = cursor; n -= pivot + 1; }
        else n = pivot;
    }
    return cursor;
}

int mdb_mid2l_insert(MDB_ID2L ids, MDB_ID2 *id) {
    unsigned x = mdb_mid2l_search(ids, id->mid);
    if (x <= ids[0].mid && ids[x].mid == id->mid) { return -1; }
    unsigned i;
    for (i = (unsigned)(++ids[0].mid); i > x; i--)
        ids[i] = ids[i - 1];
    ids[x] = *id;
    return 0;
}

int mdb_mid2l_append(MDB_ID2L ids, MDB_ID2 *id) {
    if (ids[0].mid >= UINT32_MAX) return -1;
    ids[0].mid++;
    ids[ids[0].mid] = *id;
    return 0;
}
