"""Configuration Registry API"""
import re
import jinja2
import json
import yaml

import kvstore

PREFIX = 'instances'
TMPLPREFIX = 'templates'
# Characters used to replace slash in IDs
SLASH = '--'
DOT = '__'
# Create a global kvstore client
ENDPOINT = 'http://10.112.0.101:8500/v1/kv'
#ENDPOINT = 'http://127.0.0.1:8500/v1/kv'
_kv = kvstore.Client(ENDPOINT)


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


def connect(endpoint='http://127.0.0.1:8500/v1/kv'):
    """Configure a new connection to the registry"""
    ENDPOINT = endpoint
    global _kv
    _kv = kvstore.Client(ENDPOINT)


def register(name, version, description,
             template='', templatetype='json+jinja2',
             options='', orquestrator=''):
    """Register a new service template
       Supported templates: json+jinja2, yaml+jinja2
    """
    dn = '{}/{}/{}'.format(TMPLPREFIX, name, version)
    _kv.set('{}/name'.format(dn), name)
    _kv.set('{}/version'.format(dn), version)
    _kv.set('{}/description'.format(dn), description)
    _kv.set('{}/template'.format(dn), template)
    _kv.set('{}/templatetype'.format(dn), templatetype)
    _kv.set('{}/options'.format(dn), options)
    _kv.set('{}/orquestrator'.format(dn), orquestrator)
    return Template(dn)


def get_services():
    """Get the current list of registered services"""
    subtree = _kv.recurse(TMPLPREFIX)
    names = set([_parse_service_name(e) for e in subtree.keys()])
    return list(names)

def get_service_versions(service):
    """Get the list of registered versions for a given service"""
    subtree = _kv.recurse("{}/{}".format(TMPLPREFIX, service))
    versions = set([_parse_service_version(e, service) for e in subtree.keys()])
    return list(versions)

def get_service_template(name, version):
    """Get the service template object for a given service"""
    dn = '{}/{}/{}'.format(TMPLPREFIX, name, version)
    return Template(dn)


def deregister(name, version):
    """Deregister a given service template"""
    dn = '{}/{}/{}'.format(TMPLPREFIX, name, version)
    _kv.delete(dn, recursive=True)


def instantiate(user=None, framework=None, flavour=None, options=None):
    """Register a new instance using information from the service template"""
    service = get_service_template(framework, flavour)
    templateopts = json.loads(service.options)
    if not valid(options, templateopts):
        raise InvalidOptionsError()

    mergedopts = _merge(templateopts)
    mergedopts.update(options)

    # Generate instanceid DN
    prefix = '{}/{}/{}/{}'.format(PREFIX, user, framework, flavour)
    try:
        instanceid = _generate_id(prefix)
    except kvstore.KeyDoesNotExist:
        instanceid = 1
    dn = '{}/{}'.format(prefix, instanceid)

    t = jinja2.Template(service.template)
    # TODO: Decide the global variables to pass to the template
    rendered = t.render(opts=mergedopts, user=user, servicename=framework, version=flavour,
                        instancedn=dn, instancename=dn.replace('/', '_').replace('.', '-'))
    if service.templatetype == 'json+jinja2':
        data = json.loads(rendered)
    elif service.templatetype == 'yaml+jinja2':
        data = yaml.load(rendered)
    else:
        raise UnsupportedTemplateFormatError('type: {}'.format(service.templatetype))

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
    for k in kvinfo:
        _kv.set(k, kvinfo[k])


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
    """Check if var has a value typethat can dumped directly using str()"""
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


def _generate_id(prefix):
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


def _obstain_dns(user=None, service=None, version=None):
    """ Get all the dns using parameters as filters (e.g.: gluster instances of a user)"""
    # FIXME not finished
    if user:
        dn = '{}/{}'.format(PREFIX, user)

    if service:
        dn = '{}/{}'.format(dn, service)

    if version:
        dn = '{}/{}'.format(dn, version)

    # FIXME this may not escalate with hundreds of instances
    subtree = _kv.recurse(dn)
    dns = set([_parse_cluster_dn(e) for e in subtree.keys()])

    # FIXME a None always seems to appear
    return [dn for dn in dns if dn is not None]
    #return dns


def get_cluster_instance(user=None, service=None, flavour=None, id=None, dn=None):
    """Get the properties of a given instance of service"""
    if not dn:
        dn = '{}/{}/{}/{}/{}'.format(PREFIX, user, service, flavour, id)
    return Cluster(dn)

def get_cluster_instances(user=None, service=None, version=None):
    """Get a list of instances filtered by user, framework and version"""
    instances = _obstain_dns(user, service, version)
    return [get_cluster_instance(dn=instance) for instance in instances]

def _parse_id(route, prefix):
    pattern = prefix + r'/([^/]+)'
    m = re.match(pattern, route)
    if m:
        return int(m.group(1))
    else:
        return 0


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
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist as e:
            raise KeyDoesNotExist(e.message)

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

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type,
            "mode": self.mode,
            "origin": self.origin,
            "destination": self.destination
        }


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
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist as e:
            raise KeyDoesNotExist(e.message)

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

    def to_dict(self):
        return {
            #"name": self.name, #FIXME name or networkname
            "device": self.device,
            "bridge": self.bridge,
            "address": self.address,
            "netmask": self.netmask,
            "gateway": self.gateway
        }


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
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist as e:
            raise KeyDoesNotExist(e.message)

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

    @property
    def cluster(self):
        """Contains the cluster instance to which this node belgons to"""
        clusterdn = extract_clusterdn_from_nodedn(self._endpoint)
        return Cluster(clusterdn)

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return 'Node({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint

    def to_dict(self):
        return {
            "name": self.name,
            "cpu": self.cpu,
            "mem": self.mem,
            "host": self.host,
            "status": self.status,
            "disks": [disk.to_dict() for disk in self.disks],
            "networks": [network.to_dict() for network in self.networks]
        }


def extract_clusterdn_from_nodedn(nodedn):
    """Extract the cluster DN from the given node DN"""
    m = re.search(r'^(.*)/nodes/[^/]+$', nodedn)
    return m.group(1)


class Service(object):
    """Represents a service"""
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Service, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist as e:
            raise KeyDoesNotExist(e.message)

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

    def to_dict(self):
        return {
            "name": self.name,
            "status": self.status
        }


class Cluster(object):
    """Represents a cluster instance"""
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Cluster, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist as e:
            raise KeyDoesNotExist(e.message)

    def __setattr__(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    # FIXME Temporary fix for networks setting, check if it is needed
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

    def to_dict(self):
        return {
            "instance_name": self.instance_name,
            "nodes": [node.to_dict() for node in self.nodes],
            "services": [service.to_dict() for service in self.services]
        }


class Template(object):
    """Represents a service template"""
    def __init__(self, endpoint):
        # Avoid infinite recursion reading self._endpoint
        super(Template, self).__setattr__('_endpoint', endpoint.rstrip('/'))

    def __getattr__(self, name):
        try:
            return _kv.get('{0}/{1}'.format(self._endpoint, name))
        except kvstore.KeyDoesNotExist as e:
            raise KeyDoesNotExist(e.message)

    def __setattr__(self, name, value):
        _kv.set('{0}/{1}'.format(self._endpoint, name), value)

    def __str__(self):
        return str(self._endpoint)

    def __repr__(self):
        return 'Template({})'.format(self._endpoint)

    def __eq__(self, other):
        return self._endpoint == other._endpoint

    def __lt__(self, other):
        return self._endpoint < other._endpoint

    def to_dict(self):
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "options": self.options,
            "template": self.template,
            "orquestrator": self.orquestrator
        }


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


def _parse_service_version(endpoint, service):
    """Parse the service version part of a given endpoint"""
    m = re.match(r'^{}/{}/([^/]+)'.format(TMPLPREFIX, service), endpoint)
    return m.group(1)


def _parse_service_name(endpoint):
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
