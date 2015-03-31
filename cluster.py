from node import ValNode
from fabric.api import *
from subprocess import Popen
from cassandra.cluster import Cluster


class ValCluster(object):
    
    def __init__(self, nodelist):
        self.nodelist = []
        self.hosts = nodelist
        for item in nodelist:
            self.nodelist.append(ValNode(item))
            
    def bootstrap(self, version):
        local("cstar_perf_bootstrap -v " + version)

    def get_nodes(self):
        return self.nodelist

    def get_session(self, nodes, keyspace=None, exclusive=False):
        node_ips = []
        for node in nodes:
            node_ips.append(node.get_address())

        if exclusive = True:
            wlrr = WhiteListRoundRobinPolicy([node_ip])
            cluster = Cluster(node_ips, load_balancing_policy=wlrr)
            session = cluster.connect()
        else:
            cluster = Cluster(node_ips)
            session = cluster.connect()
        
        if keyspace is not None:
            session.execute('USE %s' % keyspace)
        return session

    def stress(self, command, nodes, parallel=False):
        base = 'JAVA_HOME=~/fab/java ~/fab/stress/cassandra-2.1/tools/bin/cassandra-stress'
        hosts = ""
        for node in nodes:
            if hosts != '':
                hosts += "," + node.get_address()
            else:
                hosts = node.get_address()
        hosts = "-node " + hosts
        line = "{base} {cmd} {hosts}".format(base=base, cmd=command, hosts=hosts)
        if parallel == True:
            p = Popen(line, shell=True)
        else:
            local(line)