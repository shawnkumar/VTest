from fabric import *

class NNode:
    
    env.hosts()

    def __init__(self, address):
        self.address = address
        env.hosts = [address]

    def start(self):
        run()