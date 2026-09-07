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
        treap.c
        metrics.c
        discovery.c)

if(WIN32)
    target_link_libraries(cetcd_base PUBLIC dnsapi ws2_32)
elseif(NOT APPLE)
    target_link_libraries(cetcd_base PUBLIC resolv)
endif()

if(NOT WIN32)
    target_link_libraries(cetcd_base PUBLIC m)
endif()
