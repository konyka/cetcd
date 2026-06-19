cetcd_add_module(auth
    SOURCES
        auth.c
    PUBLIC_DEPS
        cetcd_base
    PRIVATE_DEPS
)

# Link OpenSSL for SHA-256 password hashing (optional — falls back to
# a non-cryptographic hash when unavailable).
find_package(PkgConfig REQUIRED)
pkg_check_modules(OPENSSL IMPORTED_TARGET openssl)
if(OPENSSL_FOUND)
    target_link_libraries(cetcd_auth PRIVATE PkgConfig::OPENSSL)
    target_compile_definitions(cetcd_auth PRIVATE CETCD_HAS_OPENSSL=1)
endif()
