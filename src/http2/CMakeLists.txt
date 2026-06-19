cetcd_add_module(http2
    SOURCES
        http2.c
    PUBLIC_DEPS
        cetcd_base
    PRIVATE_DEPS
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(NGHTTP2 IMPORTED_TARGET libnghttp2)
if(NGHTTP2_FOUND)
    target_link_libraries(cetcd_http2 PUBLIC PkgConfig::NGHTTP2)
    target_compile_definitions(cetcd_http2 PUBLIC CETCD_HAS_NGHTTP2=1)
else()
    message(WARNING "nghttp2 not found — HTTP/2 module will be stub-only")
endif()
