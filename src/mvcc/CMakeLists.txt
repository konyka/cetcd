cetcd_add_module(mvcc
    SOURCES
        mvcc.c
    PUBLIC_DEPS
        cetcd_base
    PRIVATE_DEPS
        cetcd_backend
)
