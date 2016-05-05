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
    prefix = '{}/{}/{}/{}'.format(PREFIX, user, framework, flavour)
    try:
        instanceid = _generate_id(prefix)
    except kvstore.KeyDoesNotExist:
        instanceid = 1
    prefix = '{}/{}'.format(prefix, instanceid)
    prefix_nodes = '{}/{}'.format(prefix, 'nodes')
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


def _dump_node(node, prefix):
    """A node can contain k/v pairs, and also non-nested dictionaries and lists"""
    for k in node:
        v = node[k]
        if isinstance(v, str):
            _kv.set('{}/{}'.format(prefix, k), v)
        elif isinstance(v, dict):
            _dump_simple_dict(v, '{}/{}'.format(prefix, k))
        elif isinstance(v, list) or isinstance(v, tuple):
            _dump_simple_list(v, '{}/{}'.format(prefix, k))


class Disk(object):
    """Represents a disk"""
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
        return 'Node({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint


class Node(object):
    """Represents a node
    
    To set the disks use:
        node.disks = [
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
        disks = set([_parse_disk(e) for e in subtree.keys()])
        return [Disk(d) for d in disks]

    @disks.setter
    def disks(self, disks):
        basedn = '{0}/{1}'.format(self._endpoint, 'disks')
        _kv.delete(basedn, recursive=True)
        for disk in disks:
            diskdn = '{}/{}'.format(basedn, disk['name'])
            _kv.set('{}/origin'.format(diskdn), disk['origin'])
            _kv.set('{}/destination'.format(diskdn), disk['destination'])
            _kv.set('{}/mode'.format(diskdn), disk['mode'])
            _kv.set('{}/name'.format(diskdn), disk['name'])

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

    @property
    def nodes(self):
        subtree = _kv.recurse(self._endpoint + '/nodes')
        nodes = {_parse_node(e) for e in subtree.keys() if not e.endswith("/nodes/")}
        x = [Node(e) for e in nodes]
        return x

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
