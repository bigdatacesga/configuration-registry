"""Tests for the generic service discovery API"""
import unittest

import kvstore
import registry

MASTER0 = {
    'status': 'pending',
    'mem': '2048',
    'cpu': '1',
    'name': 'master0.local',
    'id': '',
    'address': '',
    'host': '',
    'services': ['service0', 'service1'],
    'disks': {
        'disk1': {
            'origin': '/data/1/instance-jlopez-cdh-5.7.0-1',
            'destination': '/data/1',
            'mode': 'rw',
            },
        'disk2': {
            'origin': '/data/2/instance-jlopez-cdh-5.7.0-1',
            'destination': '/data/2',
            'mode': 'rw',
            },
        },
}

SLAVE0 = {
    'status': 'pending',
    'mem': '2048',
    'cpu': '1',
    'name': 'slave0.local',
    'id': '',
    'address': '',
    'host': '',
    'services': ['service1'],
    'disks': {
        'disk1': {
            'origin': '/data/1/instance-jlopez-cdh-5.7.0-1',
            'destination': '/data/1',
            'mode': 'rw',
            },
        'disk2': {
            'origin': '/data/2/instance-jlopez-cdh-5.7.0-1',
            'destination': '/data/2',
            'mode': 'rw',
            },
        },
}

SLAVE1 = {
    'status': 'deployed',
    'mem': '2048',
    'cpu': '1',
    'name': 'slave1.local',
    'id': '1a2b3c4e',
    'address': '10.112.200.101',
    'host': 'c13-1.local',
    'services': ['service1'],
}

SERVICE0 = {
    'status': 'pending',
    'nodes': ['master0'],
    'heap': '2048',
    'workers': '11',
}

SERVICE1 = {
    'status': 'pending',
    'nodes': ['slave0', 'slave1'],
    'heap': '2048',
    'disks': '11',
}

#BASEDN = 'instances/cdh/5.7.0/1'
BASEDN = 'clusters'

REGISTRY = {BASEDN: {
    'cluster1': {
        'nodes': {
            'master0': MASTER0,
            'slave0': SLAVE0,
            'slave1': SLAVE1
        },
        'services': {
            'service0': SERVICE0,
            'service1': SERVICE1
        },
        'status': 'running'
    }
}}


class KVMock(object):
    """Mock KV store for testing"""
    def __init__(self, data):
        self._data = data

    def get(self, key):
        key = key.strip('/')
        fields = key.split('/')
        value = self._data
        for f in fields:
            value = value[f]
        return value

    def set(self, key, value):
        key = key.strip('/')
        fields = key.split('/')
        prop = self._data
        for f in fields[:-1]:
            try:
                prop = prop[f]
            except KeyError:
                prop[f] = {}
                prop = prop[f]
        prop[fields[-1]] = value

    def recurse(self, key):
        key = key.strip('/')
        fields = key.split('/')
        subtree = self._data
        for f in fields:
            try:
                subtree = subtree[f]
            except KeyError:
                raise kvstore.KeyDoesNotExist
        result = {}
        for e in subtree:
            result['{0}/{1}'.format(key, e)] = ''
        return result

    def delete(self, key, recursive=False):
        if not recursive:
            raise NotImplementedError
        key = key.strip('/')
        fields = key.split('/')
        prop = self._data
        for f in fields[:-1]:
            prop = prop[f]
        del prop[fields[-1]]


class RegistryNodeTestCase(unittest.TestCase):

    def setUp(self):
        REGISTRY_COPY = REGISTRY.copy()
        # Mock internal kvstore in the registry
        registry._kv = KVMock(REGISTRY_COPY)

    def tearDown(self):
        pass

    def test_get_node_status(self):
        node = registry.Node(BASEDN + '/cluster1/nodes/master0')
        expected = REGISTRY[BASEDN]['cluster1']['nodes']['master0']['status']
        status = node.status
        self.assertEqual(status, expected)

    def test_set_node_status(self):
        node = registry.Node(BASEDN + '/cluster1/nodes/master0')
        node.status = 'configured'
        self.assertEqual(node.status, 'configured')

    def test_get_node_name(self):
        node = registry.Node(BASEDN + '/cluster1/nodes/master0')
        expected = REGISTRY[BASEDN]['cluster1']['nodes']['master0']['name']
        name = node.name
        self.assertEqual(name, expected)

    def test_set_node_name(self):
        node = registry.Node(BASEDN + '/cluster1/nodes/master0')
        node.name = 'new.local'
        self.assertEqual(node.name, 'new.local')

    def test_get_node_services(self):
        basedn = BASEDN + '/cluster1/nodes/master0'
        basedn_services = BASEDN + '/cluster1/services'
        node = registry.Node(basedn)
        services = REGISTRY[BASEDN]['cluster1']['nodes']['master0']['services']
        expected = [
            registry.Service('{}/{}'.format(basedn_services, n)) for n in services]
        self.assertEqual(sorted(node.services), sorted(expected))

    def test_set_node_services(self):
        basedn = BASEDN + '/cluster1/nodes/master0'
        basedn_services = BASEDN + '/cluster1/services'
        node = registry.Service(basedn)
        expected = [registry.Service('{}/service0'.format(basedn_services)),
                    registry.Service('{}/service1'.format(basedn_services))]
        node.services = expected
        self.assertEqual(sorted(node.services), sorted(expected))

    def test_get_node_disks(self):
        basedn = BASEDN + '/cluster1/nodes/master0'
        basedn_disks = basedn + '/disks'
        node = registry.Node(basedn)
        disks = REGISTRY[BASEDN]['cluster1']['nodes']['master0']['disks'].keys()
        expected = [
            registry.Disk('{}/{}'.format(basedn_disks, d)) for d in disks]
        self.assertEqual(sorted(node.disks), sorted(expected))

    def test_set_node_disks_one_disk(self):
        basedn = BASEDN + '/cluster1/nodes/master0'
        expected = [{
            'name': 'disk1',
            'origin': '/data/1/instances-jlopez-cdh-5.7.0-1',
            'destination': '/data/1',
            'mode': 'rw'}, ]
        node = registry.Node(basedn)
        node.disks = expected
        disk = node.disks[0]
        print node.disks
        print 'DEBUG: ' + str(disk)
        self.assertEqual(disk.origin, expected['origin'])

    def test_set_node_disks_two_disks(self):
        basedn = BASEDN + '/cluster1/nodes/master0'
        basedn_disks = basedn + '/disks'
        expected = [
            {
                'name': 'disk1',
                'origin': '/data/1/instances-jlopez-cdh-5.7.0-1',
                'destination': '/data/1',
                'mode': 'rw'
            },
            {
                'name': 'disk2',
                'origin': '/data/2/instances-jlopez-cdh-5.7.0-1',
                'destination': '/data/2',
                'mode': 'rw'
            },
        ]
        node = registry.Node(basedn)
        node.disks = expected
        self.assertEqual(node.disks, sorted(expected))


class RegistryServiceTestCase(unittest.TestCase):

    def setUp(self):
        REGISTRY_COPY = REGISTRY.copy()
        # Mock internal kvstore in the registry
        registry._kv = KVMock(REGISTRY_COPY)

    def tearDown(self):
        pass

    def test_get_service_status(self):
        service = registry.Service(BASEDN + '/cluster1/services/service0')
        expected = REGISTRY[BASEDN]['cluster1']['services']['service0']['status']
        status = service.status
        self.assertEqual(status, expected)

    def test_set_service_status(self):
        service = registry.Service(BASEDN + '/cluster1/services/service0')
        service.status = 'configured'
        self.assertEqual(service.status, 'configured')

    def test_get_service_heap(self):
        service = registry.Service(BASEDN + '/cluster1/services/service0')
        expected = REGISTRY[BASEDN]['cluster1']['services']['service0']['heap']
        heap = service.heap
        self.assertEqual(heap, expected)

    def test_set_service_heap(self):
        service = registry.Service(BASEDN + '/cluster1/services/service0')
        expected = '1234'
        service.heap = expected
        self.assertEqual(service.heap, expected)

    def test_get_service_nodes(self):
        basedn = BASEDN + '/cluster1/services/service0'
        basedn_nodes = BASEDN + '/cluster1/nodes'
        service = registry.Service(basedn)
        nodes = REGISTRY[BASEDN]['cluster1']['services']['service0']['nodes']
        expected = [
            registry.Node('{}/{}'.format(basedn_nodes, n)) for n in nodes]
        self.assertEqual(sorted(service.nodes), sorted(expected))

    def test_set_service_nodes(self):
        basedn = BASEDN + '/cluster1/services/service0'
        basedn_nodes = BASEDN + '/cluster1/nodes'
        service = registry.Service(basedn)
        expected = [registry.Node('{}/master0'.format(basedn_nodes)),
                    registry.Node('{}/slave1'.format(basedn_nodes))]
        service.nodes = expected
        self.assertEqual(sorted(service.nodes), sorted(expected))


class RegistryClusterTestCase(unittest.TestCase):

    def setUp(self):
        REGISTRY_COPY = REGISTRY.copy()
        # Mock internal kvstore in the registry
        registry._kv = KVMock(REGISTRY_COPY)

    def tearDown(self):
        pass

    def test_get_cluster_status(self):
        cluster = registry.Cluster('/clusters/cluster1')
        expected = REGISTRY['clusters']['cluster1']['status']
        status = cluster.status
        self.assertEqual(status, expected)

    def test_get_cluster_nodes(self):
        cluster = registry.Cluster(BASEDN + '/cluster1')
        nodes = REGISTRY[BASEDN]['cluster1']['nodes'].keys()
        expected = [
            registry.Node('{}/cluster1/nodes/{}'.format(BASEDN, e)) for e in nodes]
        self.assertEqual(sorted(cluster.nodes), sorted(expected))

    def test_get_cluster_services(self):
        cluster = registry.Cluster('/clusters/cluster1')
        services = REGISTRY['clusters']['cluster1']['services'].keys()
        expected = [
            registry.Node('clusters/cluster1/services/{0}'.format(e)) for e in services]
        self.assertEqual(sorted(cluster.services), sorted(expected))


class RegistryRegistrationTestCase(unittest.TestCase):

    def setUp(self):
        REGISTRY_COPY = REGISTRY.copy()
        # Mock internal kvstore in the registry
        registry._kv = KVMock(REGISTRY_COPY)

    def tearDown(self):
        pass

    def test_register_new_cluster_instance_returns_dn(self):
        nodes = REGISTRY['clusters']['cluster1']['nodes']
        services = REGISTRY['clusters']['cluster1']['services']
        dn = registry.register(user='jlopez', framework='a', flavour='1.0.0', nodes=nodes,
                               services=services)
        expected = registry.PREFIX + '/jlopez/a/1.0.0/1'
        self.assertEqual(dn, expected)

    def test_register_two_cluster_instances(self):
        nodes = REGISTRY['clusters']['cluster1']['nodes']
        services = REGISTRY['clusters']['cluster1']['services']
        dn = registry.register(user='jlopez', framework='a', flavour='1.0.0', nodes=nodes,
                               services=services)
        expected = registry.PREFIX + '/jlopez/a/1.0.0/1'
        self.assertEqual(dn, expected)
        dn = registry.register(user='jlopez', framework='a', flavour='1.0.0', nodes=nodes,
                               services=services)
        expected = registry.PREFIX + '/jlopez/a/1.0.0/2'
        self.assertEqual(dn, expected)

    def test_get_cluster_instance(self):
        nodes = REGISTRY['clusters']['cluster1']['nodes']
        services = REGISTRY['clusters']['cluster1']['services']
        dn = registry.register(user='jlopez', framework='a', flavour='1.0.0', nodes=nodes,
                               services=services)
        instance = registry.get_cluster_instance(dn=dn)
        expected_dn = registry.PREFIX + '/jlopez/a/1.0.0/1'
        expected_nodes = [
            registry.Node('{}/nodes/{}'.format(expected_dn, e)) for e in nodes
        ]
        expected_services = [
            registry.Service('{}/services/{}'.format(expected_dn, e)) for e in services
        ]
        self.assertEqual(sorted(instance.nodes), sorted(expected_nodes))
        self.assertEqual(sorted(instance.services), sorted(expected_services))

    def test_parse_id(self):
        route = 'instances/jlopez/cdh/5.7.0/99/nodes/master0/status'
        prefix = 'instances/jlopez/cdh/5.7.0'
        iid = registry._parse_id(route, prefix)
        self.assertEqual(iid, 99)


class RegistryUtilsTestCase(unittest.TestCase):

    def setUp(self):
        REGISTRY_COPY = REGISTRY.copy()
        # Mock internal kvstore in the registry
        registry._kv = KVMock(REGISTRY_COPY)

    def tearDown(self):
        pass

    def test_parse_cluster_dn_four_fields(self):
        dn = 'instances/cdh/5.7.0/1/nodes/node0/services'
        expected = 'instances/cdh/5.7.0/1'
        result = registry._parse_cluster_dn(dn)
        self.assertEqual(result, expected)

    def test_parse_cluster_dn_one_field(self):
        dn = 'clusters/cluster1/nodes/node0/services'
        expected = 'clusters/cluster1'
        result = registry._parse_cluster_dn(dn)
        self.assertEqual(result, expected)

    def test_parse_disk_middle(self):
        dn = 'instances/cdh/5.7.0/1/nodes/node0/disks/disk99/mode'
        expected = 'instances/cdh/5.7.0/1/nodes/node0/disks/disk99'
        result = registry._parse_disk(dn)
        self.assertEqual(result, expected)

    def test_parse_disk_end(self):
        dn = 'instances/cdh/5.7.0/1/nodes/node0/disks/disk99'
        expected = 'instances/cdh/5.7.0/1/nodes/node0/disks/disk99'
        result = registry._parse_disk(dn)
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
