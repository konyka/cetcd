cetcd_add_module(io
    SOURCES
        loop.c
        co.c
        tcp.c
        timer.c
        async.c
    PRIVATE_DEPS
        uv_a
        libco
        cetcd_base
)

# libuv and libco headers
target_include_directories(cetcd_io PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/libuv/include
    ${CMAKE_SOURCE_DIR}/third_party/libco
)
