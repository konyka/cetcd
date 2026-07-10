#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#  define _POSIX_C_SOURCE 200809L
#endif
#include "cetcd/backend.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <lmdb.h>

/* Internal structures */
struct cetcd_backend {
    MDB_env *env;
    char *path;
};

struct cetcd_txn {
    cetcd_backend *be;
    MDB_txn *txn;
};

static int _lmdb_error(int rc) {
    if (rc == MDB_SUCCESS) return CETCD_OK;
    return CETCD_ERR_IO;
}

cetcd_backend *cetcd_backend_open(const cetcd_backend_config *cfg) {
    if (!cfg || !cfg->path) return NULL;
    cetcd_backend *be = (cetcd_backend*)calloc(1, sizeof(*be));
    if (!be) return NULL;
    be->path = strdup(cfg->path);
    if (!be->path) { free(be); return NULL; }
    if (mdb_env_create(&be->env) != MDB_SUCCESS) { free(be->path); free(be); return NULL; }
    mdb_env_set_mapsize(be->env, cfg->map_size ? cfg->map_size : 16*1024*1024);
    if (cfg->max_dbs > 0) mdb_env_set_maxdbs(be->env, cfg->max_dbs);
    int rc = mdb_env_open(be->env, be->path, 0, 0664);
    if (rc != MDB_SUCCESS) { mdb_env_close(be->env); free(be->path); free(be); return NULL; }
    return be;
}

void cetcd_backend_close(cetcd_backend *be) {
    if (!be) return;
    if (be->env) mdb_env_close(be->env);
    if (be->path) free(be->path);
    free(be);
}

cetcd_txn *cetcd_txn_begin(cetcd_backend *be, bool read_only) {
    if (!be || !be->env) return NULL;
    cetcd_txn *txn = (cetcd_txn*)calloc(1, sizeof(*txn));
    if (!txn) return NULL;
    if (mdb_txn_begin(be->env, NULL, read_only ? MDB_RDONLY : 0, &txn->txn) != MDB_SUCCESS) {
        free(txn); return NULL;
    }
    txn->be = be;
    return txn;
}

int cetcd_txn_commit(cetcd_txn *txn) {
    if (!txn) return CETCD_ERR_INVAL;
    int rc = mdb_txn_commit(txn->txn);
    free(txn);
    return _lmdb_error(rc);
}

void cetcd_txn_abort(cetcd_txn *txn) {
    if (!txn) return;
    mdb_txn_abort(txn->txn);
    free(txn);
}

/* Helper to open bucket DBI for a given txn */
static int _get_dbi(MDB_txn *txn, const char *bucket, MDB_dbi *dbi) {
    if (!bucket || !dbi) return -1;
    MDB_txn *t = txn;
    return mdb_dbi_open(t, bucket, MDB_CREATE, dbi) != MDB_SUCCESS ? -1 : 0;
}

static int _put_internal(cetcd_txn *txn, const char *bucket,
                         const uint8_t *key, size_t key_len,
                         const uint8_t *val, size_t val_len) {
    MDB_dbi dbi;
    if (!_get_dbi(txn->txn, bucket, &dbi)) {
        MDB_val mkey = {.mv_data = (void*)key, .mv_size = key_len };
        MDB_val mval = {.mv_data = (void*)val, .mv_size = val_len };
        int rc = mdb_put(txn->txn, dbi, &mkey, &mval, 0);
        return _lmdb_error(rc);
    }
    return CETCD_ERR_IO;
}

static int _get_internal(cetcd_txn *txn, const char *bucket,
                         const uint8_t *key, size_t key_len,
                         uint8_t **val, size_t *val_len) {
    MDB_dbi dbi;
    if (!_get_dbi(txn->txn, bucket, &dbi)) {
        MDB_val mkey = {.mv_data = (void*)key, .mv_size = key_len };
        MDB_val mval;
        int rc = mdb_get(txn->txn, dbi, &mkey, &mval);
        if (rc != MDB_SUCCESS) { *val = NULL; *val_len = 0; return CETCD_ERR_NOTFOUND; }
        *val_len = (size_t)mval.mv_size;
        *val = (uint8_t*)malloc(*val_len);
        if (*val && mval.mv_data) memcpy(*val, mval.mv_data, *val_len);
        return CETCD_OK;
    }
    return CETCD_ERR_IO;
}


/* Public KV operations (auto-commit) */
int cetcd_backend_put(cetcd_backend *be, const char *bucket,
                       const uint8_t *key, size_t key_len,
                       const uint8_t *val, size_t val_len) {
    int rc = -1;
    cetcd_txn *txn = cetcd_txn_begin(be, false);
    if (!txn) return CETCD_ERR_IO;
    rc = _put_internal(txn, bucket, key, key_len, val, val_len);
    if (rc == CETCD_OK) rc = cetcd_txn_commit(txn);
    else cetcd_txn_abort(txn);
    return rc;
}

int cetcd_backend_get(cetcd_backend *be, const char *bucket,
                       const uint8_t *key, size_t key_len,
                       uint8_t **val, size_t *val_len) {
    int rc = -1;
    cetcd_txn *txn = cetcd_txn_begin(be, true);
    if (!txn) return CETCD_ERR_IO;
    rc = _get_internal(txn, bucket, key, key_len, val, val_len);
    cetcd_txn_abort(txn);
    return rc;
}

int cetcd_backend_del(cetcd_backend *be, const char *bucket,
                       const uint8_t *key, size_t key_len) {
    int rc = CETCD_ERR_IO;
    cetcd_txn *txn = cetcd_txn_begin(be, false);
    if (!txn) return CETCD_ERR_IO;
    MDB_dbi dbi;
    if (!_get_dbi(txn->txn, bucket, &dbi)) {
        MDB_val mkey = {.mv_data = (void*)key, .mv_size = key_len };
        int r = mdb_del(txn->txn, dbi, &mkey, NULL);
        rc = _lmdb_error(r);
        cetcd_txn_commit(txn);
        return rc;
    }
    cetcd_txn_abort(txn);
    return CETCD_ERR_IO;
}

int cetcd_backend_put2(cetcd_backend *be,
                       const char *bucket1,
                       const uint8_t *key1, size_t key1_len,
                       const uint8_t *val1, size_t val1_len,
                       const char *bucket2,
                       const uint8_t *key2, size_t key2_len,
                       const uint8_t *val2, size_t val2_len) {
    if (!be || !bucket1 || !key1 || !bucket2 || !key2 || !val2) return CETCD_ERR_INVAL;
    cetcd_txn *txn = cetcd_txn_begin(be, false);
    if (!txn) return CETCD_ERR_IO;
    int rc;
    if (val1) {
        rc = _put_internal(txn, bucket1, key1, key1_len, val1, val1_len);
    } else {
        MDB_dbi dbi;
        if (_get_dbi(txn->txn, bucket1, &dbi) != 0) {
            cetcd_txn_abort(txn);
            return CETCD_ERR_IO;
        }
        MDB_val mkey = {.mv_data = (void *)key1, .mv_size = key1_len};
        int r = mdb_del(txn->txn, dbi, &mkey, NULL);
        /* Missing key is OK when mirroring an MVCC delete. */
        rc = (r == MDB_SUCCESS || r == MDB_NOTFOUND) ? CETCD_OK : CETCD_ERR_IO;
    }
    if (rc != CETCD_OK) { cetcd_txn_abort(txn); return rc; }
    rc = _put_internal(txn, bucket2, key2, key2_len, val2, val2_len);
    if (rc == CETCD_OK) rc = cetcd_txn_commit(txn);
    else cetcd_txn_abort(txn);
    return rc;
}

int cetcd_backend_del_n(cetcd_backend *be,
                        const char *bucket,
                        const uint8_t *const *keys,
                        const size_t *key_lens,
                        size_t n,
                        const char *meta_bucket,
                        const uint8_t *meta_key, size_t meta_key_len,
                        const uint8_t *meta_val, size_t meta_val_len) {
    if (!be || !bucket || !meta_bucket || !meta_key || !meta_val) return CETCD_ERR_INVAL;
    if (n > 0 && (!keys || !key_lens)) return CETCD_ERR_INVAL;
    cetcd_txn *txn = cetcd_txn_begin(be, false);
    if (!txn) return CETCD_ERR_IO;
    for (size_t i = 0; i < n; i++) {
        if (!keys[i] || key_lens[i] == 0) continue;
        MDB_dbi dbi;
        if (_get_dbi(txn->txn, bucket, &dbi) != 0) {
            cetcd_txn_abort(txn);
            return CETCD_ERR_IO;
        }
        MDB_val mkey = {.mv_data = (void *)keys[i], .mv_size = key_lens[i]};
        int r = mdb_del(txn->txn, dbi, &mkey, NULL);
        if (r != MDB_SUCCESS && r != MDB_NOTFOUND) {
            cetcd_txn_abort(txn);
            return CETCD_ERR_IO;
        }
    }
    int rc = _put_internal(txn, meta_bucket, meta_key, meta_key_len, meta_val, meta_val_len);
    if (rc == CETCD_OK) rc = cetcd_txn_commit(txn);
    else cetcd_txn_abort(txn);
    return rc;
}

int cetcd_txn_put(cetcd_txn *txn, const char *bucket,
                   const uint8_t *key, size_t key_len,
                   const uint8_t *val, size_t val_len) {
    if (!txn) return CETCD_ERR_INVAL;
    MDB_dbi dbi;
    if (_get_dbi(txn->txn, bucket, &dbi)) return CETCD_ERR_IO;
    MDB_val mkey = {.mv_data = (void*)key, .mv_size = key_len };
    MDB_val mval = {.mv_data = (void*)val, .mv_size = val_len };
    int rc = mdb_put(txn->txn, dbi, &mkey, &mval, 0);
    return _lmdb_error(rc);
}

int cetcd_txn_get(cetcd_txn *txn, const char *bucket,
                 const uint8_t *key, size_t key_len,
                 uint8_t **val, size_t *val_len) {
    if (!txn) return CETCD_ERR_INVAL;
    MDB_dbi dbi;
    if (_get_dbi(txn->txn, bucket, &dbi)) return CETCD_ERR_IO;
    MDB_val mkey = {.mv_data = (void*)key, .mv_size = key_len };
    MDB_val mval;
    int rc = mdb_get(txn->txn, dbi, &mkey, &mval);
    if (rc != MDB_SUCCESS) return CETCD_ERR_NOTFOUND;
    *val_len = (size_t)mval.mv_size;
    *val = (uint8_t*)malloc(*val_len);
    if (*val == NULL) return CETCD_ERR_NOMEM;
    if (mval.mv_data && *val_len > 0) memcpy(*val, mval.mv_data, *val_len);
    return CETCD_OK;
}

int cetcd_txn_del(cetcd_txn *txn, const char *bucket,
                   const uint8_t *key, size_t key_len) {
    if (!txn) return CETCD_ERR_INVAL;
    MDB_dbi dbi;
    if (!_get_dbi(txn->txn, bucket, &dbi)) {
        MDB_val mkey = {.mv_data = (void*)key, .mv_size = key_len };
        int rc = mdb_del(txn->txn, dbi, &mkey, NULL);
        return _lmdb_error(rc);
    }
    return CETCD_ERR_IO;
}

uint64_t cetcd_backend_size(cetcd_backend *be) {
    if (!be || !be->env) return 0;
    MDB_stat st;
    memset(&st, 0, sizeof(st));
    int rc = mdb_env_stat(be->env, &st);
    if (rc != MDB_SUCCESS) return 0;
    /* Return the total page usage: ms_branch_pages + ms_leaf_pages +
     * ms_overflow_pages. This gives an approximate on-disk size in pages.
     * For actual byte size, multiply by the environment page size. */
    uint64_t total_pages = (uint64_t)st.ms_branch_pages +
                           (uint64_t)st.ms_leaf_pages +
                           (uint64_t)st.ms_overflow_pages;
    return total_pages * (uint64_t)st.ms_psize;
}

int cetcd_backend_foreach(cetcd_backend *be, const char *bucket,
                           cetcd_backend_iter_fn fn, void *udata) {
    if (!be || !be->env || !bucket || !fn) return CETCD_ERR_INVAL;
    cetcd_txn *txn = cetcd_txn_begin(be, true);
    if (!txn) return CETCD_ERR_IO;
    MDB_dbi dbi;
    /* Read-only: do not pass MDB_CREATE (requires a write txn). */
    int open_rc = mdb_dbi_open(txn->txn, bucket, 0, &dbi);
    if (open_rc == MDB_NOTFOUND) {
        cetcd_txn_abort(txn);
        return CETCD_OK; /* bucket never created → empty */
    }
    if (open_rc != MDB_SUCCESS) {
        cetcd_txn_abort(txn);
        return CETCD_ERR_IO;
    }
    MDB_cursor *cur = NULL;
    if (mdb_cursor_open(txn->txn, dbi, &cur) != MDB_SUCCESS) {
        cetcd_txn_abort(txn);
        return CETCD_ERR_IO;
    }
    MDB_val mkey, mval;
    int rc = mdb_cursor_get(cur, &mkey, &mval, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
        bool cont = fn((const uint8_t *)mkey.mv_data, (size_t)mkey.mv_size,
                       (const uint8_t *)mval.mv_data, (size_t)mval.mv_size,
                       udata);
        if (!cont) break;
        rc = mdb_cursor_get(cur, &mkey, &mval, MDB_NEXT);
    }
    mdb_cursor_close(cur);
    cetcd_txn_abort(txn);
    return CETCD_OK;
}
