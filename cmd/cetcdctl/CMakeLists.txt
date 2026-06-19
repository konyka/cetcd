add_executable(cetcdctl main.c)
target_compile_definitions(cetcdctl PRIVATE _POSIX_C_SOURCE=200809L)
target_link_libraries(cetcdctl
    PRIVATE
        cetcd::base
)
target_include_directories(cetcdctl PRIVATE ${CMAKE_SOURCE_DIR}/include)
install(TARGETS cetcdctl RUNTIME DESTINATION bin)
