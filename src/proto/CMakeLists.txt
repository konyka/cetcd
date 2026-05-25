cetcd_add_module(proto
    SOURCES
        kv.pb-c.c
        rpc.pb-c.c
        auth.pb-c.c
        codec.c
    PRIVATE_DEPS
        protobuf-c
        cetcd_base
)

target_include_directories(cetcd_proto PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/protobuf-c
)
