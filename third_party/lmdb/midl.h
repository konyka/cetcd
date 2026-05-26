#ifndef _MDB_IDL_H_
#define _MDB_IDL_H_

#include <stddef.h>
#include <stdint.h>

typedef unsigned long MDB_ID;

typedef MDB_ID *MDB_IDL;

typedef struct MDB_ID2 {
    MDB_ID mid;
    void *mptr;
} MDB_ID2;

typedef MDB_ID2 *MDB_ID2L;

#define MDB_IDL_LOGN	16
#define MDB_IDL_DBSIZE	 (1<<MDB_IDL_LOGN)
#define MDB_IDL_UM_SIZE	 (UINT16_C(1)<<(MDB_IDL_LOGN+1))
#define MDB_IDL_SZOF(o)	 ((o) * sizeof(MDB_ID))
#define MDB_IDL_IS_DYNAMIC(ids)	 (*(ids) >= MDB_IDL_DBSIZE)
#define MDB_IDL_RANGE(ids, f, l)	 ((ids)[0] = (f), (ids)[1] = (l))
#define MDB_IDL_FIRST(ids)	 ((ids)[1])
#define MDB_IDL_LAST(ids)		 ((ids)[(ids)[0]])
#define MDB_IDL_LEN(ids)		 ((ids)[0])
#define MDB_IDL_ZERO(ids)		 ((ids)[0] = 0)

MDB_IDL mdb_midl_alloc(int num);
void mdb_midl_free(MDB_IDL ids);
void mdb_midl_shrink(MDB_IDL *idp);
int mdb_midl_need(MDB_IDL *idp, unsigned num);
int mdb_midl_append(MDB_IDL *idp, MDB_ID id);
int mdb_midl_append_ids(MDB_IDL *idp, MDB_ID *ids, int n);
void mdb_midl_xmerge(MDB_IDL idl, MDB_IDL merge);
void mdb_midl_sort(MDB_IDL ids);
unsigned mdb_mid2l_search(MDB_ID2L ids, MDB_ID id);
int mdb_mid2l_insert(MDB_ID2L ids, MDB_ID2 *id);
int mdb_mid2l_append(MDB_ID2L ids, MDB_ID2 *id);

#endif
