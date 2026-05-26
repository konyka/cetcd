cetcd_add_module(raft
    SOURCES
        raft.c
        log.c
        storage.c
    PRIVATE_DEPS
        cetcd_base
)
