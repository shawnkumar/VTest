from node import NNode
from fabric.api import *

class CCluster(object):
    
    env.hosts = []

    def __init__(self, nodelist):
        self.nodelist = []
        for item in nodelist:
            self.nodelist.append(NNode(item))

    def bootstrap(self, version):
        env.hosts = self.nodelist
        local(cstar_perf_bootstrap + " " + version)