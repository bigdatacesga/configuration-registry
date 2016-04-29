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


class Node(object):
    """Represents a node"""
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
        services = subtree.keys()
        return [get_endpoint_last_element(e) for e in services]

    @services.setter
    def services(self, services):
        _kv.delete('{0}/{1}'.format(self._endpoint, 'services'), recursive=True)
        for s in services:
            _kv.set('{0}/{1}/{2}'.format(self._endpoint, 'services', s), '')

    def __str__(self):
        return str(self._endpoint)

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
        nodes = subtree.keys()
        return [get_endpoint_last_element(e) for e in nodes]

    @nodes.setter
    def nodes(self, nodes):
        _kv.delete('{0}/{1}'.format(self._endpoint, 'nodes'), recursive=True)
        for s in nodes:
            _kv.set('{0}/{1}/{2}'.format(self._endpoint, 'nodes', s), '')

    def __str__(self):
        return str(self._endpoint)

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
        nodes = {_parse_node(e) for e in subtree.keys()}
        return [Node(e) for e in nodes]

    @property
    def services(self):
        subtree = _kv.recurse(self._endpoint + '/services')
        services = {_parse_service(e) for e in subtree.keys()}
        return [Service(e) for e in services]

    def __str__(self):
        return str(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint


def get_endpoint_last_element(endpoint):
    """Get the last element of a given endpoint"""
    return endpoint.rstrip('/').split('/')[-1]


def _parse_service(endpoint):
    """Parse the service part of a given enpoint"""
    m = re.match(r'^(.*/services/[^/]+)', endpoint)
    return m.group(1)


def _parse_node(endpoint):
    """Parse the node part of a given enpoint"""
    m = re.match(r'^(.*/nodes/[^/]+)', endpoint)
    return m.group(1)
