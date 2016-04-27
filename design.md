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




