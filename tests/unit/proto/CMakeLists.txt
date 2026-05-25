cetcd_add_unit_test(proto_kv    MODULE proto SOURCES test_kv.c)
cetcd_add_unit_test(proto_rpc   MODULE proto SOURCES test_rpc.c)
cetcd_add_unit_test(proto_codec MODULE proto SOURCES test_codec.c)

set_property(TARGET test_proto_kv test_proto_rpc test_proto_codec
    APPEND PROPERTY INCLUDE_DIRECTORIES
    ${CMAKE_SOURCE_DIR}/src/proto
    ${CMAKE_SOURCE_DIR}/third_party/protobuf-c/protobuf-c
)
