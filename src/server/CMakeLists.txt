cetcd_add_module(server
    SOURCES
        server.c
    PUBLIC_DEPS
        cetcd_base
    PRIVATE_DEPS
        cetcd_v3rpc
        cetcd_raft
        cetcd_mvcc
        cetcd_lease
        cetcd_auth
        cetcd_peer
        cetcd_snap
        cetcd_wal
        cetcd_backend
        cetcd_io
)

target_compile_definitions(cetcd_server PRIVATE _POSIX_C_SOURCE=200809L)
target_include_directories(cetcd_server PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/libuv/include
    ${CMAKE_SOURCE_DIR}/src/io
)
