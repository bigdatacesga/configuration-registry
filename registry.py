"""Configuration Registry API"""
import re
import jinja2
import json
import yaml
from concurrent.futures import ThreadPoolExecutor

import kvstore

PREFIX = 'clusters'
TMPLPREFIX = 'products'
# Characters used to replace slash in IDs
SLASH = '--'
DOT = '__'
# Create a global kvstore client
ENDPOINT = 'http://mesosmaster:8500/v1/kv'
#ENDPOINT = 'http://127.0.0.1:8500/v1/kv'
_kv = kvstore.Client(ENDPOINT)


def connect(endpoint='http://127.0.0.1:8500/v1/kv'):
    """Configure a new connection to the registry"""
    ENDPOINT = endpoint
    global _kv
    _kv = kvstore.Client(ENDPOINT)


def register(name, version, description,
             template='', options='', orquestrator='',
             templatetype='json+jinja2'):
    """Register a new product

       A product includes:
         - name
         - version
         - description
         - template: a json template of the service
         - options: a dict with the keys required, optional,
            advanced and descriptions
         - orquestrator: a init script that supports start,
            stop, and restart
         - tempatetype: json+jinja2 or yaml+jinja2
    """
    dn = '{}/{}/{}'.format(TMPLPREFIX, name, version)
    _kv.set('{}/name'.format(dn), name)
    _kv.set('{}/version'.format(dn), version)
    _kv.set('{}/description'.format(dn), description)
    _kv.set('{}/template'.format(dn), template)
    _kv.set('{}/templatetype'.format(dn), templatetype)
    _kv.set('{}/options'.format(dn), options)
    _kv.set('{}/orquestrator'.format(dn), orquestrator)
    return Product(dn)


def deregister(name, version):
    """Deregister a given service template"""
    dn = '{}/{}/{}'.format(TMPLPREFIX, name, version)
    _kv.delete(dn, recursive=True)


def instantiate(user=None, product=None, version=None, options=None):
    """Register a new instance using information from the service template"""
    product = get_product(product, version)
    templateopts = json.loads(product.options)
    if not valid(options, templateopts):
        raise InvalidOptionsError()

    mergedopts = _merge(templateopts)
    mergedopts.update(options)

    # Generate instanceid DN
    prefix = '{}/{}/{}/{}'.format(PREFIX, user, product, version)
    try:
        instanceid = generate_id(prefix)
    except kvstore.KeyDoesNotExist:
        instanceid = 1
    dn = '{}/{}'.format(prefix, instanceid)

    t = jinja2.Template(product.template)
    rendered = t.render(opts=mergedopts, user=user, product=product, version=version,
                        clusterdn=dn, clusterid=id_from(dn))
    if product.templatetype == 'json+jinja2':
        data = json.loads(rendered)
    elif product.templatetype == 'yaml+jinja2':
        data = yaml.load(rendered)
    else:
        raise UnsupportedTemplateFormatError('type: {}'.format(product.templatetype))

    kvinfo = {}
    _populate(kvinfo, using=data, prefix=dn)
    save(kvinfo)
    return Cluster(dn)


def deinstantiate(user, framework, flavour, instanceid):
    """Deinstantiate (remove) a given cluster instance"""
    dn = '{}/{}/{}/{}/{}'.format(PREFIX, user, framework, flavour, instanceid)
    _kv.delete(dn, recursive=True)


def save(kvinfo):
    """Save kvinfo in the k/v store"""
    # Parallel version
    with ThreadPoolExecutor(max_workers=8) as executor:
        [executor.submit(_kv.set, k, v) for k, v in kvinfo.items()]
    # Sequential version
    #for k, v in kvinfo.items():
        #_kv.set(k, v)


def get_product(name=None, version=None, dn=None):
    """Get a product proxy object"""
    if not dn:
        dn = '{}/{}/{}'.format(TMPLPREFIX, name, version)
    return Product(dn)


def get_cluster(user=None, product=None, version=None, id=None, dn=None):
    """Get the a cluster instance proxy object"""
    if not dn:
        dn = '{}/{}/{}/{}/{}'.format(PREFIX, user, product, version, id)
    return Cluster(dn)


def query_clusters(user=None, service=None, version=None):
    """Get a list of clusters filtered by user, product and version

    The query parameters should be provided hierarquically, for example:
        query_clusters(): returns all clusters
        query_clusters(user): returns all clusters of the given user
        query_clusters(user, product): given user and product
        query_clusters(user, product, version): given user, product, version
    """
    try:
        clusters = _filter_cluster_endpoints(user, service, version)
        return [get_cluster(dn=dn) for dn in clusters]
    except kvstore.KeyDoesNotExist:
        return None


def query_products(product=None, version=None):
    """Get a list of products that can be filtered by product and version"""
    try:
        products = _filter_product_endpoints(product, version)
        return [get_product(dn=dn) for dn in products]
    except kvstore.KeyDoesNotExist:
        return None

# TODO: To be removed DEPRECATED
def get_products():
    """Get the list of registered products"""
    subtree = _kv.recurse(TMPLPREFIX)
    names = set([parse_product_name(e) for e in subtree.keys()])
    return list(names)


# TODO: To be removed DEPRECATED
def get_product_versions(service):
    """Get the list of registered versions for a given service"""
    subtree = _kv.recurse("{}/{}".format(TMPLPREFIX, service))
    versions = set([parse_product_version(e, service) for e in subtree.keys()])
    return list(versions)


class Proxy(object):
    """Base class for Proxy objects

    Acts as a proxy for the k/v store backend.

    __serializable__ defines the fields to return by to_dict()
    __readonly__ defines read only fields for __setattr__
    """

    __serializable__ = ('dn', 'name')
    __readonly__ = ('name')

    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Proxy, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist as e:
            raise KeyDoesNotExist(e.message)

    def __setattr__(self, name, value):
        if name in self.__class__.__readonly__:
            raise ReadOnlyAttributeError(name)
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    @property
    def dn(self):
        return self._endpoint

    @property
    def name(self):
        return parse_last_field(self._endpoint)

    def get(self, name, default=None):
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist:
            return default

    def set(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint

    def to_dict(self):
        return {k: self.get(k) for k in self.__class__.__serializable__}


class Service(Proxy):
    """Represents a service"""
    __serializable__ = ('dn', 'name', 'status')
    __readonly__ = ('name', 'nodes')

    @property
    def nodes(self):
        subtree = _kv.recurse(self._endpoint + '/nodes')
        nodes = [parse_last_field(e) for e in subtree.keys()]
        clusterdn = _parse_cluster_dn(self._endpoint)
        return [Node('{}/nodes/{}'.format(clusterdn, n)) for n in nodes]


class Cluster(Proxy):
    """Represents a cluster instance"""
    __serializable__ = ('dn', 'name', 'status')
    __readonly__ = ('name', 'nodes', 'services')

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


class Product(Proxy):
    """Represents a Product"""
    __serializable__ = ('dn', 'name', 'version', 'description')
    __readonly__ = ('name')


class Disk(Proxy):
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
    __serializable__ = ('dn', 'name', 'type', 'mode', 'origin', 'destination')
    __readonly__ = ('name')


class Network(Proxy):
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
    __serializable__ = ('dn', 'name', 'device', 'bridge', 'address', 'netmask', 'gateway')
    __readonly__ = ('name')


class Node(Proxy):
    """Represents a node"""
    __serializable__ = ('dn', 'name', 'cpu', 'mem', 'host', 'status')
    __readonly__ = ('name', 'services', 'disks', 'networks', 'cluster', 'tags')

    @property
    def services(self):
        subtree = _kv.recurse(self._endpoint + '/services')
        services = [parse_last_field(e) for e in subtree.keys()]
        clusterdn = _parse_cluster_dn(self._endpoint)
        return [Service('{}/services/{}'.format(clusterdn, s)) for s in services]

    @property
    def disks(self):
        subtree = _kv.recurse(self._endpoint + '/disks')
        disks = set([_parse_disk(e) for e in subtree.keys()])
        return [Disk(d) for d in disks]

    @property
    def networks(self):
        subtree = _kv.recurse(self._endpoint + '/networks')
        networks = set([_parse_network(e) for e in subtree.keys()])
        return [Network(n) for n in sorted(networks)]

    @property
    def tags(self):
        dn = '{0}/tags'.format(self._endpoint)
        return [x.strip() for x in _kv.get(dn).split(',')]

    @property
    def cluster(self):
        """Contains the cluster instance to which this node belgons to"""
        clusterdn = extract_clusterdn_from_nodedn(self._endpoint)
        return Cluster(clusterdn)


class InvalidOptionsError(Exception):
    pass


class NestedListsNotSupportedError(Exception):
    pass


class UnsupportedTypeError(Exception):
    pass


class UnsupportedTemplateFormatError(Exception):
    pass


class KeyDoesNotExist(Exception):
    pass


class ReadOnlyAttributeError(Exception):
    pass


def extract_clusterdn_from_nodedn(nodedn):
    """Extract the cluster DN from the given node DN"""
    m = re.search(r'^(.*)/nodes/[^/]+$', nodedn)
    return m.group(1)


def _populate(result, using, prefix=''):
    """Converts a data dict in a flat key:value data structure

       Naming the input data as using is just a convenient way to
       define the clear intent when calling this function that
       has the weird behaviour of returning the result inside one
       of the arguments
    """
    data = using

    if isvalue(data):
        result[prefix] = data
    elif islist(data):
        for e in data:
            if isvalue(e):
                result['{}/{}'.format(prefix, e)] = ''
            else:
                raise NestedListsNotSupportedError(
                    'prefix: {}, element: {}'.format(prefix, e))
    elif isdict(data):
        for k in data:
            path = '{}/{}'.format(prefix, k)
            v = data[k]
            if isvalue(v):
                result[path] = v
            elif isdumpable(v):
                _populate(result, v, path)
            else:
                raise UnsupportedTypeError(
                    'path: {}, key: {}, value: {}, type: {}'
                    .format(path, k, v, type(v)))
    else:
        raise UnsupportedTypeError('data: {}, type: {}'.format(data, type(data)))


def isvalue(var):
    """Check if var has a value type that can dumped directly using str()"""
    for t in (str, unicode, int, float, long, bool):
        if isinstance(var, t):
            return True
    return False


def isdumpable(data):
    """Check if data has a sequence type that can be dumped"""
    for t in (list, tuple, dict, set):
        if isinstance(data, t):
            return True
    return False


def islist(data):
    """Check if data is a sequence of value elements"""
    for t in (list, tuple, set):
        if isinstance(data, t):
            return True
    return False


def isdict(data):
    """Check if data offers a dictonary interface"""
    for t in (dict, ):
        if isinstance(data, t):
            return True
    return False


def generate_id(prefix):
    """Generate a new unique ID for the new instance"""
    subtree = _kv.recurse(prefix)
    instances = subtree.keys()
    used_ids = {_parse_id(e, prefix) for e in instances}
    return max(used_ids) + 1


def valid(options, templateopts):
    """Verify that all required options in the template are present"""
    required = templateopts['required'].keys()
    for k in required:
        if k not in options:
            return False
    return True


def _merge(options):
    """Merge the input options into one dict"""
    merged = {}
    for t in ('required', 'optional', 'advanced'):
        merged.update(options[t])
    return merged


def _parse_cluster_dn(endpoint):
    """Parse the cluster base DN of a given endpoint"""
    prefix = PREFIX + '/'
    location = endpoint.replace(prefix, '')
    fields = location.split('/')
    if len(location) < 4:
        return None
    return prefix + '/'.join(fields[:4])


def _parse_product_dn(endpoint):
    """Parse the product DN of a given endpoint"""
    prefix = TMPLPREFIX + '/'
    location = endpoint.replace(prefix, '')
    fields = location.split('/')
    if len(location) < 2:
        return None
    return prefix + '/'.join(fields[:2])


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


def parse_product_version(endpoint, service):
    """Parse the service version part of a given endpoint"""
    m = re.match(r'^{}/{}/([^/]+)'.format(TMPLPREFIX, service), endpoint)
    return m.group(1)


def parse_product_name(endpoint):
    """Parse the service name part of a given endpoint"""
    m = re.match(r'^{}/([^/]+)'.format(TMPLPREFIX), endpoint)
    return m.group(1)


def id_from(dn):
    """Convert a DN string in an ID string

    Basically the ID string is equivalent to a DN but without
    certain characters that can cause problems like '/'
    """
    return dn.replace('/', SLASH).replace('.', DOT)


def dn_from(id):
    """Convert an ID string into a DN string

    Basically the ID string is equivalent to a DN but without
    certain characters that can cause problems like '/'
    """
    return id.replace(DOT, '.').replace(SLASH, '/')


def _filter_cluster_endpoints(user=None, product=None, version=None):
    """ Get a list of filtered cluster endpoints using parameters as filters"""
    basedn = PREFIX
    if user:
        basedn = '{}/{}'.format(basedn, user)
        if product:
            basedn = '{}/{}'.format(basedn, product)
            if version:
                basedn = '{}/{}'.format(basedn, version)

    subtree = _kv.recurse(basedn)
    clusters = set([_parse_cluster_dn(e) for e in subtree.keys()])

    return [dn for dn in clusters]


def _filter_product_endpoints(product=None, version=None):
    """ Get a list of filtered product endpoints using parameters as filters"""
    basedn = TMPLPREFIX
    if product:
        basedn = '{}/{}'.format(basedn, product)
        if version:
            basedn = '{}/{}'.format(basedn, version)

    subtree = _kv.recurse(basedn)
    products = set([_parse_product_dn(e) for e in subtree.keys()])

    return [dn for dn in products]


def parse_name(endpoint):
    """Parse the last element of a given endpoint"""
    return endpoint.rstrip('/').split('/')[-1]


def parse_last_field(endpoint):
    """Parse the last element of a given endpoint"""
    return endpoint.rstrip('/').split('/')[-1]


def _parse_id(route, prefix):
    pattern = prefix + r'/([^/]+)'
    m = re.match(pattern, route)
    if m:
        return int(m.group(1))
    else:
        return 0
