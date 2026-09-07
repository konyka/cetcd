add_executable(cetcdctl main.c
    ${CMAKE_SOURCE_DIR}/src/peer/cluster_spec.c
    ${CMAKE_SOURCE_DIR}/src/snap/snap.c
    ${CMAKE_SOURCE_DIR}/src/server/auto_compact.c
    ${CMAKE_SOURCE_DIR}/src/server/corrupt_check.c)
target_compile_definitions(cetcdctl PRIVATE _POSIX_C_SOURCE=200809L)
target_link_libraries(cetcdctl
    PRIVATE
        cetcd::base
        cetcd::tls
)
target_include_directories(cetcdctl PRIVATE ${CMAKE_SOURCE_DIR}/include)
install(TARGETS cetcdctl RUNTIME DESTINATION bin)
