cetcd_add_unit_test(peer_sanity MODULE peer SOURCES test_peer.c EXTRA_DEPS cetcd_backend)
cetcd_add_unit_test(cluster_spec MODULE peer SOURCES test_cluster_spec.c)
cetcd_add_unit_test(reconfig MODULE peer SOURCES test_reconfig.c)
