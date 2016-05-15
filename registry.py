"""Configuration Registry API"""
import re

import kvstore

#PREFIX = 'frameworks'
PREFIX = 'instances'
# Create a global kvstore client
#ENDPOINT = 'http://10.112.0.101:8500/v1/kv'
ENDPOINT = 'http://127.0.0.1:8500/v1/kv'
_kv = kvstore.Client(ENDPOINT)


def connect(endpoint='http://127.0.0.1:8500/v1/kv'):
    """Configure a new connection to the registry"""
    ENDPOINT = endpoint
    global _kv
    _kv = kvstore.Client(ENDPOINT)


def get_cluster_instance(user=None, framework=None, flavour=None, id=None, dn=None):
    """Get the properties of a given instance of service"""
    if not dn:
        dn = '{}/{}/{}/{}/{}'.format(PREFIX, user, framework, flavour, id)
    return Cluster(dn)


def register(user=None, framework=None, flavour=None, nodes=None, services=None):
    """Register a new instance"""
    prefix = '{}/{}/{}/{}'.format(PREFIX, user, framework, flavour)
    try:
        instanceid = _generate_id(prefix)
    except kvstore.KeyDoesNotExist:
        instanceid = 1
    prefix = '{}/{}'.format(prefix, instanceid)
    prefix_nodes = '{}/{}'.format(prefix, 'nodes')
    prefix_services = '{}/{}'.format(prefix, 'services')
    for node in nodes:
        _dump_node(nodes[node], '{}/{}'.format(prefix_nodes, node))
    for service in services:
        _dump_node(services[service], '{}/{}'.format(prefix_services, service))
    return prefix


def _generate_id(prefix):
    """Generate a new unique ID for the new instance"""
    subtree = _kv.recurse(prefix)
    instances = subtree.keys()
    used_ids = {_parse_id(e, prefix) for e in instances}
    return max(used_ids) + 1


def _parse_id(route, prefix):
    pattern = prefix + r'/([^/]+)'
    m = re.match(pattern, route)
    if m:
        return int(m.group(1))
    else:
        return 0


def _dump_simple_dict(data, prefix):
    """Dump a simple dictionary that contains only k:v pairs"""
    for k in data:
        _kv.set('{}/{}'.format(prefix, k), data[k])


def _dump_simple_list(data, prefix):
    """Dump a simple list that contains only k:v pairs"""
    for e in data:
        _kv.set('{}/{}'.format(prefix, e), '')


def _dump_list(data, prefix):
    for e in data:
        if isinstance(e, list):
            _dump_list(e, '{}/{}'.format(prefix, '_')) # list of lists?
        elif isinstance(e, dict):
            _dump_dict(e, '{}/{}'.format(prefix, e["name"]))
        elif isinstance(e, str):
            _kv.set('{}/{}'.format(prefix, e), '')


def _dump_dict(data, prefix):
    for k in data:
        v = data[k]
        if isinstance(v, list):
            _dump_list(v, '{}/{}'.format(prefix, k))
        elif isinstance(v, dict):
            _dump_dict(v, '{}/{}'.format(prefix, v["name"]))
        elif isinstance(v, basestring):
            # basestring is True for all strings, unicode, ascii...
            _kv.set('{}/{}'.format(prefix, k), v)


def _dump_node(node, prefix):
    """A node can contain k/v pairs, and also non-nested dictionaries and lists"""
    for k in node:
        v = node[k]
        if isinstance(v, str):
            _kv.set('{}/{}'.format(prefix, k), v)
        elif isinstance(v, dict):
            #_dump_simple_dict(v, '{}/{}'.format(prefix, k))
            _dump_dict(v, '{}/{}'.format(prefix, k))
        elif isinstance(v, list) or isinstance(v, tuple):
            #_dump_simple_list(v, '{}/{'.format(prefix, k))
            _dump_list(v, '{}/{}'.format(prefix, k))


class Disk(object):
    """Represents a disk

    To set the disks use:
        node.disks = [
            {
                 'name': 'disk1',
                 'origin': '/data/1/instances-jlopez-cdh-5.7.0-1',
                 'destination': '/data/1',
                 'mode': 'rw',
                 'type': 'sata',
            },
            {
                 'name': 'disk2',
                 'origin': '/data/2/instances-jlopez-cdh-5.7.0-1',
                 'destination': '/data/2',
                 'mode': 'rw'
                 'type': 'sata',
            },
        ]
    """
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Disk, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        return _kv.get('{0}/{1}'.format(self._endpoint, name))

    def __setattr__(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return 'Disk({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint


class Network(object):
    """Represents a network address

    To set the networks use:
        node.networks = [
            {
                 'name': 'eth0',
                 'device': 'eth0',
                 'bridge': 'virbrPRIVATE',
                 'address': '10.112.251.101',
                 'netmask': '16',
                 'gateway': '10.112.0.1',
            },
            {
                 'name': 'eth1',
                 'device': 'eth1',
                 'bridge': 'virbrSTORAGE',
                 'address': '10.117.251.101',
                 'netmask': '16',
                 'gateway': '',
            },
        ]
    """
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Network, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        return _kv.get('{0}/{1}'.format(self._endpoint, name))

    def __setattr__(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return 'Network({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint


class Node(object):
    """Represents a node

    It must include:
      * type: eg. docker
      * name: eg. slave5
      * clustername: eg. jlopez-cdh-5.7.0-1
      * tags: eg. ('slave', 'datanode')
      * docker_image: eg. cdh:5.7.0
      * docker_opts: specific opts
          eg. --privileged -v /sys/fs/cgroup:/sys/fs/cgroup:ro
      * disks
      * networks
      * services
    """
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Node, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        return _kv.get('{0}/{1}'.format(self._endpoint, name))

    def __setattr__(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    @property
    def services(self):
        subtree = _kv.recurse(self._endpoint + '/services')
        services = [_parse_endpoint_last_element(e) for e in subtree.keys()]
        clusterdn = _parse_cluster_dn(self._endpoint)
        return [Service('{}/services/{}'.format(clusterdn, s)) for s in services]

    @services.setter
    def services(self, services):
        _kv.delete('{0}/{1}'.format(self._endpoint, 'services'), recursive=True)
        for service in services:
            _kv.set(_parse_endpoint_last_element(service._endpoint), '')

    @property
    def disks(self):
        subtree = _kv.recurse(self._endpoint + '/disks')
        disks = set([_parse_disk(e) for e in subtree.keys() if not e.endswith("/disks/")])
        return [Disk(d) for d in disks]

    #FIXME: Temporary FIX
    def set_disks(self, disks):
        basedn = '{0}/{1}'.format(self._endpoint, 'disks')
        #_kv.delete(basedn, recursive=True)
        for disk in disks:
            diskdn = '{0}/{1}'.format(basedn, disk['name'])
            for k in disk:
                _kv.set('{0}/{1}'.format(diskdn, k), disk[k])

    @disks.setter
    def disks(self, disks):
        basedn = '{0}/{1}'.format(self._endpoint, 'disks')
        _kv.delete(basedn, recursive=True)
        for disk in disks:
            diskdn = '{0}/{1}'.format(basedn, disk['name'])
            _kv.set('{0}/origin'.format(diskdn), disk['origin'])
            _kv.set('{0}/destination'.format(diskdn), disk['destination'])
            _kv.set('{0}/mode'.format(diskdn), disk['mode'])
            _kv.set('{0}/type'.format(diskdn), disk['type'])
            _kv.set('{0}/name'.format(diskdn), disk['name'])

    @property
    def networks(self):
        subtree = _kv.recurse(self._endpoint + '/networks')
        networks = set([_parse_network(e) for e in subtree.keys()])
        return [Network(n) for n in sorted(networks)]

    #FIXME: Temporary FIX
    def set_networks(self, networks):
        basedn = '{0}/{1}'.format(self._endpoint, 'networks')
        #_kv.delete(basedn, recursive=True)
        for network in networks:
            networkdn = '{0}/{1}'.format(basedn, network['name'])
            for k in network:
                _kv.set('{0}/{1}'.format(networkdn, k), network[k])

    @networks.setter
    def networks(self, networks):
        basedn = '{0}/{1}'.format(self._endpoint, 'networks')
        _kv.delete(basedn, recursive=True)
        for network in networks:
            networkdn = '{0}/{1}'.format(basedn, network['name'])
            _kv.set('{0}/address'.format(networkdn), network['address'])
            _kv.set('{0}/bridge'.format(networkdn), network['bridge'])
            _kv.set('{0}/device'.format(networkdn), network['device'])
            _kv.set('{0}/gateway'.format(networkdn), network['gateway'])
            _kv.set('{0}/netmask'.format(networkdn), network['netmask'])
            _kv.set('{0}/name'.format(networkdn), network['name'])

    @property
    def tags(self):
        dn = '{0}/tags'.format(self._endpoint)
        return [x.strip() for x in _kv.get(dn).split(',')]

    @tags.setter
    def tags(self, tags):
        dn = '{0}/tags'.format(self._endpoint)
        value = ','.join(tags)
        _kv.set(dn, value)

    @property
    def check_ports(self):
        dn = '{0}/check_ports'.format(self._endpoint)
        return [int(x.strip()) for x in _kv.get(dn).split(',')]

    @check_ports.setter
    def check_ports(self, check_ports):
        dn = '{0}/check_ports'.format(self._endpoint)
        value = ','.join(check_ports)
        _kv.set(dn, value)

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return 'Node({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint


class Service(object):
    """Represents a service"""
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Service, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        return _kv.get('{0}/{1}'.format(self._endpoint, name))

    def __setattr__(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    @property
    def nodes(self):
        subtree = _kv.recurse(self._endpoint + '/nodes')
        nodes = [_parse_endpoint_last_element(e) for e in subtree.keys()]
        clusterdn = _parse_cluster_dn(self._endpoint)
        return [Node('{}/nodes/{}'.format(clusterdn, n)) for n in nodes]

    @nodes.setter
    def nodes(self, nodes):
        _kv.delete('{0}/{1}'.format(self._endpoint, 'nodes'), recursive=True)
        for node in nodes:
            _kv.set(_parse_endpoint_last_element(node._endpoint), '')

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return 'Service({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint


class Cluster(object):
    """Represents a cluster instance"""
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Cluster, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        return _kv.get('{0}/{1}'.format(self._endpoint, name))

    def __setattr__(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    # Temporary FIX ?
    def set_attributes(self, data):
        for k in data:
            _kv.set('{0}/{1}'.format(self._endpoint, k), data[k])

    @property
    def nodes(self):
        subtree = _kv.recurse(self._endpoint + '/nodes')
        nodes = {_parse_node(e) for e in subtree.keys() if not e.endswith("/nodes/")}
        return [Node(e) for e in nodes]

    @property
    def services(self):
        subtree = _kv.recurse(self._endpoint + '/services')
        services = {_parse_service(e) for e in subtree.keys() if not e.endswith("/services/")}
        return [Service(e) for e in services]

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return 'Cluster({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint


def _parse_endpoint_last_element(endpoint):
    """Parse the last element of a given endpoint"""
    return endpoint.rstrip('/').split('/')[-1]


def _parse_cluster_dn(endpoint):
    """Parse the cluster base DN of a given endpoint"""
    #fields = endpoint.split('/')
    #return '/'.join(fields[0:4])
    m = re.match(r'^(.+)/services/[^/]+/nodes', endpoint)
    if m:
        return m.group(1)
    m = re.match(r'^(.+)/nodes/[^/]+/services', endpoint)
    if m:
        return m.group(1)
    m = re.match(r'^(.+)/services', endpoint)
    if m:
        return m.group(1)
    m = re.match(r'^(.+)/nodes', endpoint)
    if m:
        return m.group(1)


def _parse_service(endpoint):
    """Parse the service part of a given endpoint"""
    m = re.match(r'^(.*/services/[^/]+)', endpoint)
    return m.group(1)


def _parse_node(endpoint):
    """Parse the node part of a given endpoint"""
    m = re.match(r'^(.*/nodes/[^/]+)', endpoint)
    return m.group(1)


def _parse_disk(endpoint):
    """Parse the disk part of a given endpoint"""
    m = re.match(r'^(.*/disks/[^/]+)', endpoint)
    return m.group(1)


def _parse_network(endpoint):
    """Parse the network part of a given endpoint"""
    m = re.match(r'^(.*/networks/[^/]+)', endpoint)
    return m.group(1)
