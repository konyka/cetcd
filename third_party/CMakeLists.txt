# Third-party dependencies — vendored under this directory.

# --- libuv (event loop) ---
set(LIBUV_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(UV_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(libuv)

# Ensure a consistent target name: libuv exports 'uv_a' on most platforms.
# If it only exported 'uv', create an alias.
if(NOT TARGET uv_a AND TARGET uv)
    add_library(uv_a ALIAS uv)
endif()

# --- libco (stackful coroutines) ---
add_subdirectory(libco)

# --- protobuf-c runtime (pack/unpack only, no protoc-gen-c) ---
add_subdirectory(protobuf-c/build-cmake)
