cetcd_add_module(tls
    SOURCES
        tls.c
    PUBLIC_DEPS
        cetcd_base
    PRIVATE_DEPS
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(OPENSSL IMPORTED_TARGET openssl)
if(OPENSSL_FOUND)
    target_link_libraries(cetcd_tls PRIVATE PkgConfig::OPENSSL)
    target_compile_definitions(cetcd_tls PRIVATE CETCD_HAS_OPENSSL=1)
else()
    message(WARNING "OpenSSL not found — TLS module will be stub-only")
endif()
