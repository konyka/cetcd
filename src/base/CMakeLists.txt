cetcd_add_module(base
    SOURCES
        error.c
        slice.c
        buf.c
        clock.c
        log.c
        arena.c
        slab.c
        hash.c
        hashmap.c
        treap.c)

if(NOT WIN32)
    target_link_libraries(cetcd_base PUBLIC m)
endif()
