add_library(lmdb STATIC mdb.c midl.c)
target_include_directories(lmdb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(lmdb PRIVATE _POSIX_C_SOURCE=200809L)
target_compile_options(lmdb PRIVATE -Wno-unused-parameter -Wno-sign-compare -Wno-missing-prototypes -Wno-shadow)
