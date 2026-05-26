cetcd_add_module(peer
    SOURCES
        peer.c
    PUBLIC_DEPS
        cetcd_base
    PRIVATE_DEPS
        cetcd_raft
)
