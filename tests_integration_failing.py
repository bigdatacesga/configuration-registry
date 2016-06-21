
"""Integration Tests for the generic service discovery API

In this case they do not use a Mock, they access the real Consul K/V store
"""
import unittest
import time

import kvstore
import registry

URL = 'http://10.112.0.101:8500/v1/kv'
BASEDN = '__registrytests__'

with open('service-template.json') as jsontemplate:
    TEMPLATE = jsontemplate.read()

with open('service-template.yaml') as yamltemplate:
    TEMPLATEYAML = yamltemplate.read()

with open('options.json') as optionsfile:
    OPTIONS = optionsfile.read()


class RegistryTemplatesTestCase(unittest.TestCase):

    def setUp(self):
        registry.connect(URL)
        self.PREFIX = registry.TMPLPREFIX
        self.servicename = "__unittests__"
        self.start_time = time.time()

    def tearDown(self):
        registry._kv.delete('{}/{}'.format(self.PREFIX, self.servicename), recursive=True)
        duration = time.time() - self.start_time
        print '{} took {} seconds'.format(self.id(), duration)

    def test_set_node_disks_one_disk(self):
        cluster = self._add_sample_service_instance()
        node = cluster.nodes[0]
        expected = [{
            'name': 'disk1',
            'origin': '/data/1/instances-jlopez-cdh-5.7.0-1',
            'destination': '/data/1',
            'mode': 'rw'}, ]
        #node.disks = expected
        node.set_disks(expected)
        disk = node.disks[0]
        print 'DEBUG: node.disks: {}'.format(node.disks)
        print 'DEBUG: disk: {}'.format(disk)
        self.assertEqual(disk.origin, expected['origin'])

    def _add_sample_service_instance(self):
        servicename = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATE
        templateopts = OPTIONS
        registry.register(servicename, version, description,
                          template, templateopts)
        user = 'testuser'

        options = {'slaves.number': 1}
        cluster = registry.instantiate(user, servicename, version, options)
        return cluster

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

    def test_register_new_cluster_instance_returns_dn(self):
        nodes = REGISTRY['clusters']['cluster1']['nodes']
        services = REGISTRY['clusters']['cluster1']['services']
        dn = registry.instantiate(user='jlopez', product='a', version='1.0.0', nodes=nodes,
                               services=services)
        expected = registry.PREFIX + '/jlopez/a/1.0.0/1'
        self.assertEqual(dn, expected)

    def test_register_two_cluster_instances(self):
        nodes = REGISTRY['clusters']['cluster1']['nodes']
        services = REGISTRY['clusters']['cluster1']['services']
        dn = registry.instantiate(user='jlopez', product='a', version='1.0.0', nodes=nodes,
                               services=services)
        expected = registry.PREFIX + '/jlopez/a/1.0.0/1'
        self.assertEqual(dn, expected)
        dn = registry.instantiate(user='jlopez', product='a', version='1.0.0', nodes=nodes,
                               services=services)
        expected = registry.PREFIX + '/jlopez/a/1.0.0/2'
        self.assertEqual(dn, expected)

    def test_get_cluster_instance(self):
        nodes = REGISTRY['clusters']['cluster1']['nodes']
        services = REGISTRY['clusters']['cluster1']['services']
        dn = registry.instantiate(user='jlopez', product='a', version='1.0.0', nodes=nodes,
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

    def test_get_node_networks(self):
        raise NotImplemented

    def test_set_node_networks(self):
        raise NotImplemented

    def test_get_node_tags(self):
        raise NotImplemented

    def test_set_node_tags(self):
        raise NotImplemented

if __name__ == '__main__':
    unittest.main()
