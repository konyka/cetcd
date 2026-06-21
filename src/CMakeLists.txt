# Source tree — one subdirectory per libcetcd_* module.
#
# Modules are added incrementally as phases land. Each subdirectory defines its
# own static library `cetcd_<name>` and an alias `cetcd::<name>`.

add_subdirectory(base)
add_subdirectory(proto)
add_subdirectory(io)
add_subdirectory(raft)
add_subdirectory(wal)
add_subdirectory(backend)
add_subdirectory(mvcc)
add_subdirectory(lease)
add_subdirectory(http2)
add_subdirectory(tls)
add_subdirectory(v3rpc)
add_subdirectory(peer)
add_subdirectory(auth)
add_subdirectory(snap)
add_subdirectory(server)
