from fabric.api import *
from nodetasks import *

class ValNode:
    
    def __init__(self, address):
        self.hosts = [address]

    def start(self):
        execute(start, hosts=self.hosts)

    def stop(self):
        execute(stop, hosts=self.hosts)

    def get_address(self):
        return self.hosts[0]