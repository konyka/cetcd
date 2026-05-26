add_executable(bench_micro bench_micro.c)
target_link_libraries(bench_micro
    PRIVATE
        cetcd::mvcc
        cetcd::lease
        cetcd::auth
        cetcd::snap
        cetcd::base
)
target_include_directories(bench_micro PRIVATE ${CMAKE_SOURCE_DIR}/include)
