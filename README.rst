Python Configuration Registry API
=================================

Purpose
-------
The objective of this module is to provide a common API to the configuration
registy used to store service instance properties using KeyValue stores
like consul, etcd or zookeeper.

Usage examples
--------------
Basic usage examples::

    import registry
    registry.connect()
    instance = registry.get_cluster_instance(user='jlopez', framework='cdh', flavour='5.7.0', id='1')
    # Alternatively you can retrieve it by DN
    instance = registry.get_cluster_instance(dn='jlopez/cdh/5.7.0/1')

    nodes = instance.nodes
    services = instance.services

    for node in nodes:
        print node.status

    (user, framework, flavour, id) = registry.register(user='jlopez', framework='cdh', flavour='5.7.0', nodes=nodes, services=services)
