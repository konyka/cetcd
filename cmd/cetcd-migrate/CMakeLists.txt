add_executable(cetcd-migrate main.c)

if(NOT MSVC)
    target_compile_definitions(cetcd-migrate PRIVATE _POSIX_C_SOURCE=200809L _DEFAULT_SOURCE)
endif()

target_link_libraries(cetcd-migrate
    PRIVATE
        cetcd::backend
        cetcd::base
)
target_include_directories(cetcd-migrate PRIVATE ${CMAKE_SOURCE_DIR}/include)
install(TARGETS cetcd-migrate RUNTIME DESTINATION bin)
