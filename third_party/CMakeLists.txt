# Third-party dependencies — vendored.
#
# Phase 0 has no third-party deps (libcetcd_base is freestanding-ish; only libc
# and C11 atomics). Later phases will populate this directory with libuv,
# nghttp2, LMDB, protobuf-c, libco, mimalloc, Unity, CMock.
#
# For each vendored lib we will add a thin CMakeLists wrapper that exposes a
# `cetcd::<lib>` target. Until then this file is a placeholder.

message(STATUS "third_party/: no vendored deps required at Phase 0")
