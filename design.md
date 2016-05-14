Design v1
---------

/clusters/hdp/cluster1/{nodes,services}
/clusters/mpi/cluster1/{nodes,services}

  nodes/{master0,master1,slave0,slave1}/{services,resources,name,address,host,status}
  services/yarn/master0/heap

    resources = {'cpu': '', 'mem': '', 'disk': ''}

Design v2
---------

/clusters/hdp/cluster1/{nodes,services}
/clusters/mpi/cluster1/{nodes,services}

  nodes/{master0,master1,slave0,slave1}/{services,cpu,mem,disk,name,address,host,status}
  services/yarn/{status,nodes,heap,stack,...}

Design v3
---------
/frameworks/<username>/<framework_name>/<flavour>/<instance_id>/{nodes,services}

<framework_name> : p.e.,: docker_multi, docker_single, hdp, cdh, Cassandra ...
<flavour> : docker_multi -> slurm_15.04_V1, openmpi, slurm_15.04_V2, mpich, parallelR
            hpd -> 2.4.0, 2.4.1
<instance_id> : number or string+number

/frameworks/jenes/docker_multi/slurm_15.04_V1/12/nodes 
  -> list of nodes that create the framework ({master0,master1,slave0,slave1})
  -> with recurse 

/frameworks/jenes/docker_multi/slurm_15.04_V1/12/services
  -> configuration properties of the services provided by the framework
  
  nodes/{master0,master1,slave0,slave1}/{services,cpu,mem,disk,name,address,host,status}
  services/yarn/{status,nodes,heap,stack,...}


/frameworks/jenes/multinode/slurm/12
  nodes/master0/services 
    -> services that are provided by master inside its framework
    
  services/slurmctld/nodes
    -> nodes that provide the slurmctld service in the framework

Design v4
---------
```
instances/
└── brunneis
    └── cdh
        └── 5.7.X
            └── 1
                ├── nodes
                │   └── node1
                │       ├── cpu
                │       ├── disks
                │       │   └── disk1
                │       │       ├── destination
                │       │       ├── mode
                │       │       ├── name
                │       │       ├── origin
                │       │       └── type
                │       ├── docker_image
                │       ├── docker_opts
                │       ├── host
                │       ├── id
                │       ├── mem
                │       ├── name
                │       ├── clustername
                │       ├── networks
                │       │   └── eth0
                │       │       ├── address
                │       │       ├── bridge
                │       │       ├── device
                │       │       ├── gateway
                │       │       ├── netmask
                │       │       └── network
                │       ├── services
                │       │   └── service1 : spark-master
                │       │   └── service2 : spark-history-server
                │       │   └── service3 : hbase-master
                │       │   └── service4 : hbase-thrift
                │       │   └── service5 : hbase-rest
                │       │   └── service6 : hbase-regionserver
                │       ├── status
                │       ├── port
                │       ├── check_ports
                │       └── tags
                └── services
                    └── service1
                        ├── name: name that will be given to this roledef in fabric orquestrator
                        ├── nodes
                        │   ├── node1
                        │   └── node2
                        ├── property1
                        ├── property2
                        └── status
                    └── service2
                        ├── name
                        ├── nodes
                        │   └── node1
                        ├── property1
                        ├── property2
                        └── status

```

## Docker-executor
   Node object:

    - name: name to give to the docker container
    - clustername: name of the cluster/service to which this docker belongs
    - docker_image
    - docker_opts
    - disks: Disk object list (see below)
    - networks: Network object list (see below)
    - tags: ('master', 'yarn')
    - status: pending, running, failed, stopped
    - host: docker engine where the container is running
    - id: docker id
    - port: main service port, e.g. 22
    - check_ports: list of ports to check that the container is alive

    Network object (registry.Network object):

    - device
    - address
    - bridge
    - netmask
    - gateway

    Volume object (registry.Disk object):

    - origin
    - destination
    - mode


