cetcd_add_module(auth
    SOURCES
        auth.c
    PUBLIC_DEPS
        cetcd_base
    PRIVATE_DEPS
        cetcd_backend
)

# Link OpenSSL for SHA-256 password hashing (optional — falls back to
# a non-cryptographic hash when unavailable). libxcrypt for optional bcrypt.
find_package(PkgConfig REQUIRED)
pkg_check_modules(OPENSSL IMPORTED_TARGET openssl)
if(OPENSSL_FOUND)
    target_link_libraries(cetcd_auth PRIVATE PkgConfig::OPENSSL)
    target_compile_definitions(cetcd_auth PRIVATE CETCD_HAS_OPENSSL=1)
endif()
pkg_check_modules(LIBCRYPT IMPORTED_TARGET libxcrypt)
if(LIBCRYPT_FOUND)
    target_link_libraries(cetcd_auth PRIVATE PkgConfig::LIBCRYPT)
    target_compile_definitions(cetcd_auth PRIVATE CETCD_HAS_CRYPT=1)
else()
    find_library(CRYPT_LIBRARY crypt)
    if(CRYPT_LIBRARY)
        target_link_libraries(cetcd_auth PRIVATE ${CRYPT_LIBRARY})
        target_compile_definitions(cetcd_auth PRIVATE CETCD_HAS_CRYPT=1)
    endif()
endif()
