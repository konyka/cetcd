add_executable(cetcd main.c)
target_link_libraries(cetcd
    PRIVATE
        cetcd::v3rpc
        cetcd::raft
        cetcd::peer
        cetcd::auth
        cetcd::snap
        cetcd::base
)
target_include_directories(cetcd PRIVATE ${CMAKE_SOURCE_DIR}/include)
install(TARGETS cetcd RUNTIME DESTINATION bin)
