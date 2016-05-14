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

    (user, framework, flavour, instance_id) = registry.register(user='jlopez', framework='cdh', flavour='5.7.0', nodes=nodes, services=services)

    nodes = {
        'master0': {
            'name': 'master0.local', # docker name
            'id': '', # docker id
            'status': 'pending',
            'cpu': '1',
            'mem': '2048',
            'disks': {
                'type': 'ssd',
                'number': 1,
                'disk1': '/data/1',
            },
            'networks': {
                'eth0': '10.117.253.101',
                'eth1': '10.112.253.101',
            },
            'host': '', # docker engine
            'services': ['service0', 'service1'],
        },
        'slave0': {
            'name': 'slave0.local', # docker name
            'id': '', # docker id
            'status': 'pending',
            'cpu': '1',
            'mem': '2048',
            'disks': {
                'type': 'sata',
                'number': 2,
                'disk1': '/data/1',
                'disk2': '/data/2',
            },
            'networks': {
                'eth0': '10.117.253.101',
                'eth1': '10.112.253.101',
            },
            'host': '', # docker engine
            'services': ['service2'],
        },
    }

    services = {
        'service0': {
            'status': 'pending',
            'nodes': ['master0'],
            'property1': '2048',
            'property2': '11',
        },
        'service1': {
            'status': 'running',
            'nodes': ['slave0'],
            'property1': '2048',
            'property2': '11',
        },
    }

    framework = {
        'executor': 'docker-executor',
        'image': 'gluster:2.7.0',
        'management_script': 'http://.../manage_gluster_cluster.py'
    }


Notes
-----

```
{'master0': {'cpu': '1',
  'disks': {'disk1': '/data/1', 'number': 1, 'type': 'ssd'},
  'host': '',
  'id': '',
  'mem': '2048',
  'name': 'master0.local',
  'networks': {'eth0': '10.117.253.101', 'eth1': '10.112.253.101'},
  'services': ['service0', 'service1'],
  'status': 'pending'},
 'slave0': {'cpu': '1',
  'disks': {'disk1': '/data/1',
   'disk2': '/data/2',
   'number': 2,
   'type': 'sata',
   'volumes': {'disk1': {'destination': '/data/1',
     'mode': 'rw',
     'origin': '/data/1/instances-jlopez-template-0.1.0-2'},
    'disk2': {'destination': '/data/2', 'mode': 'rw', 'origin': '/data/2'}}},
  'host': '',
  'id': '',
  'mem': '2048',
  'name': 'slave0.local',
  'networks': {'eth0': '10.117.253.101', 'eth1': '10.112.253.101'},
  'services': ['service2'],
  'status': 'pending'}}

```


Copy recursively an instance into a new one:

```
slave0 = kv.recurse('instances/jlopez/cdh/5.7.0/1/nodes/slave0')
slave1 = {k.replace('slave0', 'slave1'): slave0[k] for k in slave0.keys()}
for k in slave1.keys():
    kv.set(k, slave1[k])

```

