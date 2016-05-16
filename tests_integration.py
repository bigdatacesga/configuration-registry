"""Integration Tests for the generic service discovery API

In this case they do not use a Mock, they access the real Consul K/V store
"""
import unittest

import kvstore
import registry

URL = 'http://10.112.0.101:8500/v1/kv'
BASEDN = '__registrytests__'

TEMPLATE = """
{% set comma = joiner(",") %}
{
"nodes": {
    "master0": {
        "name": "master0", "clustername": "X", "status": "X",
        "docker_image": "X", "docker_opts": "X",
        "port": "X", "check_ports": [22, 80, 443], "tags": ["yarn", "master"],
        "cpu": 1, "mem": 1024,
        "host": "X", "id": "X", "status": "X",
        "disks": {
            "disk1": {
                "name": "disk1", "type": "ssd",
                "origin": "/data/1/{{ instancename }}",
                "destination": "/data/1", "mode": "rw"
            }
        },
        "networks": {
            "eth0": {
                "networkname": "admin", "device": "X", "bridge": "X",
                "address": "X", "gateway": "X", "netmask": "X"
            },
            "eth1": {
                "networkname": "storage", "device": "X", "bridge": "X",
                "address": "X", "gateway": "X", "netmask": "X"
            }
        },
        "services": ["yarn", "snamenode"]
    },
    "master1": {
        "name": "master1", "clustername": "X", "status": "X",
        "docker_image": "X", "docker_opts": "X",
        "port": "X", "check_ports": [22, 80, 443], "tags": ["namenode", "master"],
        "cpu": 1, "mem": 1024,
        "host": "X", "id": "X", "status": "X",
        "disks": {
            "disk1": {
                "name": "disk1", "type": "ssd",
                "origin": "/data/1/{{ instancename }}",
                "destination": "/data/1", "mode": "rw"
            }
        },
        "networks": {
            "eth0": {
                "networkname": "admin", "device": "X", "bridge": "X",
                "address": "X", "gateway": "X", "netmask": "X"
            },
            "eth1": {
                "networkname": "storage", "device": "X", "bridge": "X",
                "address": "X", "gateway": "X", "netmask": "X"
            }
        },
        "services": ["namenode"]
    },
{% for n in range(0, slaves.number) %}
    {{ comma() }} "slave{{ n }}": {
        "name": "slave{{ n }}", "clustername": "X", "status": "X",
        "docker_image": "X", "docker_opts": "X",
        "port": "X", "check_ports": [22, 4444], "tags": ["datanode", "slave"],
        "cpu": 1, "mem": 1024,
        "host": "X", "id": "X", "status": "X",
        "disks": { {% set comma = joiner(",") %}{% for k in range(0, slaves.disks) %}
            {{ comma() }} "disk{{ k }}": {
                "name": "disk{{ k }}", "type": "sata",
                "origin": "/data/{{ k }}/{{ instancename }}",
                "destination": "/data/{{ k }}", "mode": "rw"
            } {% endfor %}
        },
        "networks": {
            "eth0": {
                "networkname": "admin", "device": "X", "bridge": "X",
                "address": "X", "gateway": "X", "netmask": "X"
            },
            "eth1": {
                "networkname": "storage", "device": "X", "bridge": "X",
                "address": "X", "gateway": "X", "netmask": "X"
            }
        },
        "services": ["datanode"]
    }
{% endfor %}
},
"services": {
    "yarn": {
        "name": "yarn", "status": "X",
        "yarn.scheduler.minimum-allocation-vcores": 1,
        "nodes": ["master0"]
    },
    "datanode": {
        "name": "datanode", "status": "X",
        "dfs.blocksize": 134217728,
        "nodes": [{% set comma = joiner(",") %}{% for n in range(0, slaves.number) %}{{ comma() }}"slave{{ n }}"{% endfor %}]
    }
}

}
"""

OPTIONS = """
{
    "required": {
        "slaves.number": 4
    },
    "optional": {
        "slaves.cpu": 2,
        "slaves.mem": 2048,
        "slaves.disks": 11
    },
    "advanced": {
        "datanode.heap": 1024
    },
    "descriptions": {
        "slaves.number": "Number of slave nodes",
        "slaves.cpu": "Number of cores per slave node",
        "slaves.mem": "Memory per slave node (MB)",
        "slaves.disks": "Number of disks per slave node",
        "datanode.heap": "Max heap memory for the datanode service"
    }
}
"""


class RegistryTemplatesTestCase(unittest.TestCase):

    def setUp(self):
        registry.connect(URL)
        self.PREFIX = registry.TMPLPREFIX
        self.servicename = "__unittests__"

    def tearDown(self):
        registry._kv.delete('{}/{}'.format(self.PREFIX, self.servicename), recursive=True)

    def test_add_service_template(self):
        PREFIX = self.PREFIX
        name = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATE
        options = OPTIONS

        registry.add_service_template(name, version, description, template, options)

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

    def test_get_service_template(self):
        name = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATE
        options = OPTIONS
        registry.add_service_template(name, version, description, template, options)

        service = registry.get_service_template(name, version)

        self.assertEqual(service.name, name)
        self.assertEqual(service.version, version)
        self.assertEqual(service.description, description)
        self.assertEqual(service.template, template)
        self.assertEqual(service.options, options)

    def test_add_instance(self):
        servicename = self.servicename
        version = "0.1.0"
        description = "Unit test"
        template = TEMPLATE
        options = OPTIONS
        registry.add_service_template(servicename, version, description, template, options)
        user = 'testuser'

        cluster = registry.add_cluster_instance(user, servicename, version, options)
        # Check that template vars {{slave.nodes}} have been replaced
        raise NotImplementedError


if __name__ == '__main__':
    unittest.main()
