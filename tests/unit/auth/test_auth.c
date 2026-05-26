#include "cetcd/base.h"
#include "cetcd/auth.h"
#include "cetcd_test.h"

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

CETCD_TEST_LIST_BEGIN
    CETCD_TEST_ENTRY(auth_store_create_destroy),
    CETCD_TEST_ENTRY(auth_add_remove_user),
    CETCD_TEST_ENTRY(auth_password_check),
    CETCD_TEST_ENTRY(auth_add_remove_role),
    CETCD_TEST_ENTRY(auth_grant_revoke_role),
    CETCD_TEST_ENTRY(auth_enable_disable),
CETCD_TEST_LIST_END

CETCD_TEST_MAIN()
