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

    # Register a new service template using default template type: json+jinja2
    registry.register(name, version, description, template, options)
    # using template type: yaml+jinja2
    cluster = registry.register(name, version, description, template,
                                options, templatetype='yaml+jinja2')

    # Instantiate a new cluster from a given service template
    cluster = registry.instantiate(user, servicename, version, options)

    # Retrieve a previously instantiated cluster instance
    cluster = registry.get_cluster_instance(user='jlopez', framework='cdh', flavour='5.7.0', id='1')
    # Alternatively you can retrieve it by DN
    cluster = registry.get_cluster_instance(dn='jlopez/cdh/5.7.0/1')

    # Retrieve a previously registered Template object
    templateProxy = registry.get_service_template(name, version)
    template = templateProxy.template
    options = templateProxy.options
    description = templateProxy.description

    nodes = cluster.nodes
    services = cluster.services

    for node in nodes:
        print node.status

    nodes[0].status = 'running'

    # Deregister a service template (removes it)
    registry.deregister(service_name, service_version)

    # Deinstantiate a cluster instance (removes it)
    registry.deinstantiate(user, framework, flavour)

Notes
-----

Copy recursively an instance into a new one:

```
slave0 = kv.recurse('instances/jlopez/cdh/5.7.0/1/nodes/slave0')
slave1 = {k.replace('slave0', 'slave1'): slave0[k] for k in slave0.keys()}
for k in slave1.keys():
    kv.set(k, slave1[k])

```

Sample service Template:
------------------------

- service-template.json
- service-template.yaml

Errors
------

FIXME: yaml+jinja2 fails the test 40% of the times:
```

.E..........
======================================================================
ERROR: test_add_instance_yamltemplate (__main__.RegistryTemplatesTestCase)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "tests_integration.py", line 122, in test_add_instance_yamltemplate
    self.assertEqual(cluster.nodes[0].networks[0].networkname, 'admin')
  File "/home/jlopez/home_common/Reference/src/python/bigdata/configuration-registry/registry.py", line 372, in networks
    subtree = _kv.recurse(self._endpoint + '/networks')
  File "/home/jlopez/home_common/Reference/src/python/bigdata/configuration-registry/venv/local/lib/python2.7/site-packages/kvstore.py", line 68, in recurse
    raise KeyDoesNotExist("Key " + k + " does not exist")
KeyDoesNotExist: Key instances/testuser/__unittests__/0.1.0/44/nodes/master0/networks does not exist

----------------------------------------------------------------------
Ran 12 tests in 14.156s

FAILED (errors=1)

```

