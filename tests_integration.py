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

DEBUG = False


class RegistryTemplatesTestCase(unittest.TestCase):

    def setUp(self):
        registry.connect(URL)
        self.PREFIX = registry.TMPLPREFIX
        self.servicename = "__unittests__"
        self.start_time = time.time()

    def tearDown(self):
        registry._kv.delete('{}/{}'.format(self.PREFIX, self.servicename), recursive=True)
        duration = time.time() - self.start_time
        if DEBUG:
            print '{} took {} seconds'.format(self.id(), duration)

    def test_register(self):
        PREFIX = self.PREFIX
        name = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATE
        options = OPTIONS

        registry.register(name, version, description, template, options)

        ret_name = registry._kv.get('{}/{}/{}/name'.format(PREFIX, name, version))
        ret_version = registry._kv.get('{}/{}/{}/version'.format(PREFIX, name, version))
        ret_description = registry._kv.get('{}/{}/{}/description'.format(PREFIX, name, version))
        ret_template = registry._kv.get('{}/{}/{}/template'.format(PREFIX, name, version))
        ret_options = registry._kv.get('{}/{}/{}/options'.format(PREFIX, name, version))

        self.assertEqual(ret_name, name)
        self.assertEqual(ret_version, version)
        self.assertEqual(ret_description, description)
        self.assertEqual(ret_template, template)
        self.assertEqual(ret_options, options)

    def test_merge_options(self):
        options = {'required': {'a': 1, 'b': 2}, 'optional': {'c': 3}, 'advanced': {'d': 4}}
        merged = registry._merge(options)
        expected = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
        self.assertEqual(merged, expected)

    def test_valid_options(self):
        templateopts = {'required': {'a': 1, 'b': 2}, 'optional': {'c': 3}, 'advanced': {'d': 4}}
        options = {'a': 2, 'b': 4}
        result = registry.valid(options, templateopts)
        self.assertTrue(result)

    def test_get_service_template(self):
        name = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATE
        options = OPTIONS
        registry.register(name, version, description, template, options)

        service = registry.get_service_template(name, version)

        self.assertEqual(service.name, name)
        self.assertEqual(service.version, version)
        self.assertEqual(service.description, description)
        self.assertEqual(service.template, template)
        self.assertEqual(service.options, options)

    def test_add_instance_jsontemplate(self):
        servicename = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATE
        templateopts = OPTIONS
        registry.register(servicename, version, description,
                          template, templateopts, templatetype='json+jinja2')
        user = 'testuser'
        options = {'slaves.number': 2}
        cluster = registry.instantiate(user, servicename, version, options)

        ## expected 4 nodes: 2 slaves + 2 masters
        self.assertEqual(len(cluster.nodes), options['slaves.number'] + 2)
        self.assertEqual(len(cluster.services), 2)
        self.assertEqual(cluster.nodes[0].networks[0].networkname, 'admin')

    def test_add_instance_yamltemplate(self):
        servicename = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATEYAML
        templateopts = OPTIONS
        registry.register(servicename, version, description,
                          template, templateopts, templatetype='yaml+jinja2')
        user = 'testuser'
        options = {'slaves.number': 2}
        cluster = registry.instantiate(user, servicename, version, options)

        ## expected 4 nodes: 2 slaves + 2 masters
        self.assertEqual(len(cluster.nodes), options['slaves.number'] + 2)
        self.assertEqual(len(cluster.services), 2)
        self.assertEqual(cluster.nodes[0].networks[0].networkname, 'admin')

    def test_populate_simple(self):
        data = {'a': 1, 'b': 'hello'}
        result = {}
        registry._populate(result, using=data, prefix='X')
        expected = {
            'X/a': 1,
            'X/b': 'hello',
        }
        self.assertEqual(result, expected)

    def test_populate_simple_dict(self):
        data = {
            'dict': {'n1': 'n.n1', 'n2': 'n.n2'},
        }
        result = {}
        registry._populate(result, using=data, prefix='X')
        expected = {
            'X/dict/n1': 'n.n1',
            'X/dict/n2': 'n.n2',
        }
        self.assertEqual(result, expected)

    def test_populate_simple_list(self):
        data = {
            'list': [1, 2, 3]
        }
        result = {}
        registry._populate(result, using=data, prefix='X')
        expected = {
            'X/list/1': '',
            'X/list/2': '',
            'X/list/3': '',
        }
        self.assertEqual(result, expected)

    def test_populate_list_and_dict(self):
        data = {
            'a': 1,
            'b': 'hello',
            'dict': {'n1': 'n.n1', 'n2': 'n.n2'},
            'list': [1, 2, 3]
        }
        result = {}
        registry._populate(result, using=data, prefix='X')
        expected = {
            'X/a': 1,
            'X/b': 'hello',
            'X/dict/n1': 'n.n1',
            'X/dict/n2': 'n.n2',
            'X/list/1': '',
            'X/list/2': '',
            'X/list/3': '',
        }
        self.assertEqual(result, expected)

    def test_populate_nested_dict_and_list(self):
        data = {
            'a': 1,
            'b': 'hello',
            'dict': {
                'c': 1,
                'd': {
                    'e': 1,
                    'f': 2,
                    'g': {
                        'h': '_',
                        'j': 2
                    },
                    'list': ['a', 'b']
                }
            },
        }
        result = {}
        registry._populate(result, using=data, prefix='X')
        expected = {
            'X/a': 1,
            'X/b': 'hello',
            'X/dict/c': 1,
            'X/dict/d/e': 1,
            'X/dict/d/f': 2,
            'X/dict/d/g/h': '_',
            'X/dict/d/g/j': 2,
            'X/dict/d/list/a': '',
            'X/dict/d/list/b': '',
        }
        self.assertEqual(result, expected)

    def test_populate_services(self):
        data = {u'services': {
                    u'datanode': {
                        u'dfs.blocksize': 134217728,
                        u'name': u'datanode',
                        u'nodes': [
                            u'slave0',
                            u'slave1',
                            u'slave2'],
                        u'status': u'pending'},
                    u'yarn': {
                        u'name': u'yarn',
                        u'nodes': [u'master0'],
                        u'status': u'running',
                        u'yarn.scheduler.minimum-allocation-vcores': 1}}}
        result = {}
        registry._populate(result, using=data, prefix='X')
        expected = {
            'X/services/datanode/dfs.blocksize': 134217728,
            'X/services/datanode/name': 'datanode',
            'X/services/datanode/nodes/slave0': '',
            'X/services/datanode/nodes/slave1': '',
            'X/services/datanode/nodes/slave2': '',
            'X/services/datanode/status': 'pending',
            'X/services/yarn/name': 'yarn',
            'X/services/yarn/nodes/master0': '',
            'X/services/yarn/status': 'running',
            'X/services/yarn/yarn.scheduler.minimum-allocation-vcores': 1,
        }
        self.assertEqual(result, expected)

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

if __name__ == '__main__':
    unittest.main()
