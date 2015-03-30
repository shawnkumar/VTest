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

    def get_session(self):
        cluster = Cluster(self.hosts)
        session = cluster.connect()
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
        if parallel = True:
            p = Popen(line)
        else:
            local(line)