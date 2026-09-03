#define _POSIX_C_SOURCE 200809L
#include "cetcd/base.h"
#include "cetcd/auth.h"
#include "cetcd/backend.h"
#include "cetcd_test.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

CETCD_TEST_CASE(auth_store_create_destroy) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_NOT_NULL(s);
    CETCD_ASSERT_FALSE(cetcd_auth_is_enabled(s));
    CETCD_ASSERT_EQ_INT((int)cetcd_auth_user_count(s), 0);
    CETCD_ASSERT_EQ_INT((int)cetcd_auth_role_count(s), 0);
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_add_remove_user) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "root", "secret123"), CETCD_OK);
    CETCD_ASSERT_TRUE(cetcd_auth_has_user(s, "root"));
    CETCD_ASSERT_EQ_INT((int)cetcd_auth_user_count(s), 1);

    /* Duplicate user */
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "root", "other"), CETCD_ERR_EXISTS);

    /* Remove */
    CETCD_ASSERT_EQ_INT(cetcd_auth_remove_user(s, "root"), CETCD_OK);
    CETCD_ASSERT_FALSE(cetcd_auth_has_user(s, "root"));
    CETCD_ASSERT_EQ_INT((int)cetcd_auth_user_count(s), 0);

    /* Remove non-existent */
    CETCD_ASSERT_EQ_INT(cetcd_auth_remove_user(s, "nobody"), CETCD_ERR_NOTFOUND);

    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_password_check) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    cetcd_auth_add_user(s, "alice", "password123");

    CETCD_ASSERT_TRUE(cetcd_auth_check_password(s, "alice", "password123"));
    CETCD_ASSERT_FALSE(cetcd_auth_check_password(s, "alice", "wrongpass"));
    CETCD_ASSERT_FALSE(cetcd_auth_check_password(s, "nobody", "password123"));

    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_add_remove_role) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(
        cetcd_auth_add_role(s, "readwrite", 1, 1, "/", 1), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_auth_role_count(s), 1);

    /* Duplicate role */
    CETCD_ASSERT_EQ_INT(
        cetcd_auth_add_role(s, "readwrite", 1, 0, "/", 1), CETCD_ERR_EXISTS);

    /* Remove */
    CETCD_ASSERT_EQ_INT(cetcd_auth_remove_role(s, "readwrite"), CETCD_OK);
    CETCD_ASSERT_EQ_INT((int)cetcd_auth_role_count(s), 0);

    /* Remove non-existent */
    CETCD_ASSERT_EQ_INT(cetcd_auth_remove_role(s, "nobody"), CETCD_ERR_NOTFOUND);

    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_grant_revoke_role) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    cetcd_auth_add_user(s, "bob", "pass");
    cetcd_auth_add_role(s, "readonly", 1, 0, "/", 1);

    CETCD_ASSERT_EQ_INT(cetcd_auth_grant_role(s, "bob", "readonly"), CETCD_OK);

    /* Grant to non-existent user */
    CETCD_ASSERT_EQ_INT(cetcd_auth_grant_role(s, "nobody", "readonly"),
                        CETCD_ERR_NOTFOUND);

    /* Grant non-existent role */
    CETCD_ASSERT_EQ_INT(cetcd_auth_grant_role(s, "bob", "norole"),
                        CETCD_ERR_NOTFOUND);

    /* Revoke */
    CETCD_ASSERT_EQ_INT(cetcd_auth_revoke_role(s, "bob", "readonly"), CETCD_OK);

    /* Revoke again (not granted) */
    CETCD_ASSERT_EQ_INT(cetcd_auth_revoke_role(s, "bob", "readonly"),
                        CETCD_ERR_NOTFOUND);

    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_enable_disable) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_FALSE(cetcd_auth_is_enabled(s));
    cetcd_auth_set_enabled(s, true);
    CETCD_ASSERT_TRUE(cetcd_auth_is_enabled(s));
    cetcd_auth_set_enabled(s, false);
    CETCD_ASSERT_FALSE(cetcd_auth_is_enabled(s));
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_issue_unique_tokens) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "alice", "secret"), CETCD_OK);

    char *t1 = cetcd_auth_issue_token(s, "alice");
    char *t2 = cetcd_auth_issue_token(s, "alice");
    CETCD_ASSERT_NOT_NULL(t1);
    CETCD_ASSERT_NOT_NULL(t2);
    CETCD_ASSERT_TRUE(strlen(t1) >= 16);
    CETCD_ASSERT_TRUE(strcmp(t1, t2) != 0);
    CETCD_ASSERT_TRUE(strcmp(t1, "token") != 0);

    uint64_t now = cetcd_clock_realtime_ns();
    CETCD_ASSERT_TRUE(strcmp(cetcd_auth_user_for_token(s, t1, now), "alice") == 0);
    CETCD_ASSERT_TRUE(strcmp(cetcd_auth_user_for_token(s, t2, now), "alice") == 0);
    CETCD_ASSERT_NULL(cetcd_auth_user_for_token(s, "nope", now));
    CETCD_ASSERT_NULL(cetcd_auth_user_for_token(s, NULL, now));

    free(t1);
    free(t2);
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_token_expiry_fail_closed) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "bob", "pw"), CETCD_OK);
    cetcd_auth_set_token_ttl_ns(s, 1000000000ULL); /* 1s */

    char *tok = cetcd_auth_issue_token(s, "bob");
    CETCD_ASSERT_NOT_NULL(tok);
    uint64_t now = cetcd_clock_realtime_ns();
    CETCD_ASSERT_NOT_NULL(cetcd_auth_user_for_token(s, tok, now));
    CETCD_ASSERT_NULL(cetcd_auth_user_for_token(s, tok, now + 2000000000ULL));
    free(tok);
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_revoke_tokens_on_password_change) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    cetcd_auth_add_user(s, "carol", "old");
    char *tok = cetcd_auth_issue_token(s, "carol");
    CETCD_ASSERT_NOT_NULL(tok);
    uint64_t now = cetcd_clock_realtime_ns();
    CETCD_ASSERT_NOT_NULL(cetcd_auth_user_for_token(s, tok, now));

    CETCD_ASSERT_EQ_INT(cetcd_auth_change_password(s, "carol", "new"), CETCD_OK);
    CETCD_ASSERT_NULL(cetcd_auth_user_for_token(s, tok, now));
    free(tok);
    cetcd_auth_store_free(s);
}

static int write_hmac_key_(char *dir, size_t dirsz, char *spec, size_t specsz, const char *ttl) {
    char tmpl[] = "/tmp/cetcd-jwt-XXXXXX";
    if (!mkdtemp(tmpl)) return -1;
    snprintf(dir, dirsz, "%s", tmpl);
    char path[300];
    snprintf(path, sizeof(path), "%s/key", tmpl);
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    const char secret[] = "cetcd-hs256-secret";
    if (fwrite(secret, 1, sizeof(secret) - 1, f) != sizeof(secret) - 1) {
        fclose(f); return -1;
    }
    fclose(f);
    if (ttl && ttl[0])
        snprintf(spec, specsz, "jwt,sign-method=HS256,priv-key=%s,ttl=%s", path, ttl);
    else
        snprintf(spec, specsz, "jwt,sign-method=HS256,priv-key=%s", path);
    return 0;
}

static void cleanup_hmac_key_(const char *dir) {
    char path[300];
    snprintf(path, sizeof(path), "%s/key", dir);
    unlink(path);
    rmdir(dir);
}

CETCD_TEST_CASE(auth_jwt_hs256_roundtrip_and_tamper) {
    char dir[128], spec[400];
    CETCD_ASSERT_EQ_INT(write_hmac_key_(dir, sizeof(dir), spec, sizeof(spec), "5m"), 0);
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "alice", "secret"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_token_spec(s, spec), CETCD_OK);

    char *tok = cetcd_auth_issue_token(s, "alice");
    CETCD_ASSERT_NOT_NULL(tok);
    CETCD_ASSERT_TRUE(strchr(tok, '.') != NULL);
    uint64_t now = cetcd_clock_realtime_ns();
    CETCD_ASSERT_TRUE(strcmp(cetcd_auth_user_for_token(s, tok, now), "alice") == 0);

    size_t n = strlen(tok);
    tok[n - 1] = (char)(tok[n - 1] == 'A' ? 'B' : 'A');
    CETCD_ASSERT_NULL(cetcd_auth_user_for_token(s, tok, now));
    free(tok);

    CETCD_ASSERT_EQ_INT(cetcd_auth_set_token_spec(s, "jwt"), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_token_spec(s, "jwt,sign-method=PS256,priv-key=/dev/null"),
                        CETCD_ERR_UNSUPPORT);
    cetcd_auth_store_free(s);
    cleanup_hmac_key_(dir);
}

static int write_pem_key_(char *dir, size_t dirsz, char *spec, size_t specsz,
                          const char *alg, const char *openssl_args) {
    char tmpl[] = "/tmp/cetcd-jwt-XXXXXX";
    if (!mkdtemp(tmpl)) return -1;
    snprintf(dir, dirsz, "%s", tmpl);
    char path[300], cmd[640];
    snprintf(path, sizeof(path), "%s/key.pem", tmpl);
    snprintf(cmd, sizeof(cmd),
             "openssl genpkey %s -out '%s' >/dev/null 2>&1", openssl_args, path);
    if (system(cmd) != 0) return -1;
    snprintf(spec, specsz, "jwt,sign-method=%s,priv-key=%s,ttl=5m", alg, path);
    return 0;
}

static void cleanup_pem_key_(const char *dir) {
    char path[300];
    snprintf(path, sizeof(path), "%s/key.pem", dir);
    unlink(path);
    rmdir(dir);
}

CETCD_TEST_CASE(auth_jwt_rs256_es256_roundtrip) {
    char dir[128], spec[400];
    CETCD_ASSERT_EQ_INT(write_pem_key_(dir, sizeof(dir), spec, sizeof(spec),
        "RS256", "-algorithm RSA -pkeyopt rsa_keygen_bits:2048"), 0);
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "alice", "secret"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_token_spec(s, spec), CETCD_OK);
    char *tok = cetcd_auth_issue_token(s, "alice");
    CETCD_ASSERT_NOT_NULL(tok);
    uint64_t now = cetcd_clock_realtime_ns();
    CETCD_ASSERT_TRUE(strcmp(cetcd_auth_user_for_token(s, tok, now), "alice") == 0);
    size_t n = strlen(tok);
    tok[n - 1] = (char)(tok[n - 1] == 'A' ? 'B' : 'A');
    CETCD_ASSERT_NULL(cetcd_auth_user_for_token(s, tok, now));
    free(tok);
    cetcd_auth_store_free(s);
    cleanup_pem_key_(dir);

    CETCD_ASSERT_EQ_INT(write_pem_key_(dir, sizeof(dir), spec, sizeof(spec),
        "ES256", "-algorithm EC -pkeyopt ec_paramgen_curve:P-256"), 0);
    s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "alice", "secret"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_token_spec(s, spec), CETCD_OK);
    tok = cetcd_auth_issue_token(s, "alice");
    CETCD_ASSERT_NOT_NULL(tok);
    now = cetcd_clock_realtime_ns();
    CETCD_ASSERT_TRUE(strcmp(cetcd_auth_user_for_token(s, tok, now), "alice") == 0);
    free(tok);
    cetcd_auth_store_free(s);
    cleanup_pem_key_(dir);
}

CETCD_TEST_CASE(auth_jwt_expiry_and_stateless_password_change) {
    char dir[128], spec[400];
    CETCD_ASSERT_EQ_INT(write_hmac_key_(dir, sizeof(dir), spec, sizeof(spec), "1s"), 0);
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "bob", "old"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_token_spec(s, spec), CETCD_OK);

    char *tok = cetcd_auth_issue_token(s, "bob");
    CETCD_ASSERT_NOT_NULL(tok);
    uint64_t now = cetcd_clock_realtime_ns();
    CETCD_ASSERT_NOT_NULL(cetcd_auth_user_for_token(s, tok, now));
    CETCD_ASSERT_EQ_INT(cetcd_auth_change_password(s, "bob", "new"), CETCD_OK);
    CETCD_ASSERT_NOT_NULL(cetcd_auth_user_for_token(s, tok, now));
    CETCD_ASSERT_NULL(cetcd_auth_user_for_token(s, tok, now + 2000000000ULL));
    free(tok);
    cetcd_auth_store_free(s);
    cleanup_hmac_key_(dir);
}

CETCD_TEST_CASE(auth_admin_and_key_perm) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    cetcd_auth_add_user(s, "root", "r");
    cetcd_auth_add_user(s, "app", "a");
    cetcd_auth_add_role(s, "appread", 1, 0, "/app", 4);
    cetcd_auth_grant_role(s, "app", "appread");

    CETCD_ASSERT_TRUE(cetcd_auth_is_admin(s, "root"));
    CETCD_ASSERT_FALSE(cetcd_auth_is_admin(s, "app"));

    /* Auth disabled: allow all. */
    CETCD_ASSERT_TRUE(cetcd_auth_check_perm(s, "app", (const uint8_t *)"/x", 2, 1));

    cetcd_auth_set_enabled(s, true);
    CETCD_ASSERT_TRUE(cetcd_auth_check_perm(s, "root", (const uint8_t *)"/x", 2, 1));
    CETCD_ASSERT_TRUE(cetcd_auth_check_perm(s, "app", (const uint8_t *)"/app/k", 6, 0));
    CETCD_ASSERT_FALSE(cetcd_auth_check_perm(s, "app", (const uint8_t *)"/app/k", 6, 1));
    CETCD_ASSERT_FALSE(cetcd_auth_check_perm(s, "app", (const uint8_t *)"/other", 6, 0));
    CETCD_ASSERT_FALSE(cetcd_auth_check_perm(s, NULL, (const uint8_t *)"/app", 4, 0));
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_persist_roundtrip) {
    char path_template[] = "/tmp/cetcd-test-auth-XXXXXX";
    char *path = mkdtemp(path_template);
    CETCD_ASSERT_NOT_NULL(path);
    cetcd_backend_config cfg = {
        .path = path, .map_size = 16 * 1024 * 1024, .max_dbs = 8
    };
    cetcd_backend *be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);

    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "root", "secret"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_role(s, "rw", 1, 1, "/", 1), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_grant_role(s, "root", "rw"), CETCD_OK);
    cetcd_auth_set_enabled(s, true);
    CETCD_ASSERT_EQ_INT(cetcd_auth_save(s, be), CETCD_OK);
    cetcd_auth_store_free(s);

    cetcd_auth_store *loaded = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_load(loaded, be), CETCD_OK);
    CETCD_ASSERT_TRUE(cetcd_auth_is_enabled(loaded));
    CETCD_ASSERT_TRUE(cetcd_auth_has_user(loaded, "root"));
    CETCD_ASSERT_TRUE(cetcd_auth_check_password(loaded, "root", "secret"));
    CETCD_ASSERT_FALSE(cetcd_auth_check_password(loaded, "root", "wrong"));
    CETCD_ASSERT_EQ_INT((int)cetcd_auth_role_count(loaded), 1);
    cetcd_auth_store_free(loaded);
    cetcd_backend_close(be);
}

CETCD_TEST_CASE(auth_bcrypt_cost_rejects_out_of_range) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_bcrypt_cost(s, 3), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_bcrypt_cost(s, 32), CETCD_ERR_INVAL);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_bcrypt_cost(s, 0), CETCD_OK);
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_bcrypt_hash_and_verify) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    int rc = cetcd_auth_set_bcrypt_cost(s, 4);
    CETCD_ASSERT_EQ_INT(rc, CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "alice", "password123"), CETCD_OK);
    const cetcd_user *u = cetcd_auth_get_user(s, "alice");
    CETCD_ASSERT_NOT_NULL(u);
    CETCD_ASSERT_TRUE(u->hash_len > 32);
    CETCD_ASSERT_TRUE(cetcd_auth_check_password(s, "alice", "password123"));
    CETCD_ASSERT_FALSE(cetcd_auth_check_password(s, "alice", "wrongpass"));
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_bcrypt_verifies_existing_sha256_user) {
    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "bob", "oldpass"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_bcrypt_cost(s, 4), CETCD_OK);
    CETCD_ASSERT_TRUE(cetcd_auth_check_password(s, "bob", "oldpass"));
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "carol", "newpass"), CETCD_OK);
    CETCD_ASSERT_TRUE(cetcd_auth_check_password(s, "carol", "newpass"));
    cetcd_auth_store_free(s);
}

CETCD_TEST_CASE(auth_bcrypt_persist_roundtrip) {
    char path_template[] = "/tmp/cetcd-test-auth-bcrypt-XXXXXX";
    char *path = mkdtemp(path_template);
    CETCD_ASSERT_NOT_NULL(path);
    cetcd_backend_config cfg = {
        .path = path, .map_size = 16 * 1024 * 1024, .max_dbs = 8
    };
    cetcd_backend *be = cetcd_backend_open(&cfg);
    CETCD_ASSERT_NOT_NULL(be);

    cetcd_auth_store *s = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_set_bcrypt_cost(s, 4), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_add_user(s, "root", "secret"), CETCD_OK);
    CETCD_ASSERT_EQ_INT(cetcd_auth_save(s, be), CETCD_OK);
    cetcd_auth_store_free(s);

    cetcd_auth_store *loaded = cetcd_auth_store_new();
    CETCD_ASSERT_EQ_INT(cetcd_auth_load(loaded, be), CETCD_OK);
    CETCD_ASSERT_TRUE(cetcd_auth_check_password(loaded, "root", "secret"));
    CETCD_ASSERT_FALSE(cetcd_auth_check_password(loaded, "root", "wrong"));
    const cetcd_user *u = cetcd_auth_get_user(loaded, "root");
    CETCD_ASSERT_NOT_NULL(u);
    CETCD_ASSERT_TRUE(u->hash_len > 32);
    cetcd_auth_store_free(loaded);
    cetcd_backend_close(be);
}

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(auth_store_create_destroy),
    CETCD_TEST_ENTRY(auth_add_remove_user),
    CETCD_TEST_ENTRY(auth_password_check),
    CETCD_TEST_ENTRY(auth_add_remove_role),
    CETCD_TEST_ENTRY(auth_grant_revoke_role),
    CETCD_TEST_ENTRY(auth_enable_disable),
    CETCD_TEST_ENTRY(auth_issue_unique_tokens),
    CETCD_TEST_ENTRY(auth_token_expiry_fail_closed),
    CETCD_TEST_ENTRY(auth_revoke_tokens_on_password_change),
    CETCD_TEST_ENTRY(auth_jwt_hs256_roundtrip_and_tamper),
    CETCD_TEST_ENTRY(auth_jwt_rs256_es256_roundtrip),
    CETCD_TEST_ENTRY(auth_jwt_expiry_and_stateless_password_change),
    CETCD_TEST_ENTRY(auth_admin_and_key_perm),
    CETCD_TEST_ENTRY(auth_persist_roundtrip),
    CETCD_TEST_ENTRY(auth_bcrypt_cost_rejects_out_of_range),
    CETCD_TEST_ENTRY(auth_bcrypt_hash_and_verify),
    CETCD_TEST_ENTRY(auth_bcrypt_verifies_existing_sha256_user),
    CETCD_TEST_ENTRY(auth_bcrypt_persist_roundtrip),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
