from cluster import ValCluster
from node import ValNode
from utils import *
from complexqueryer import *
from unittest import assertTrue
from multiprocessing import Process
import time

class TestValidation():

    def stress_validation_test(self):
        cluster = ValCluster(get_ctool_nodes())
        cluster.bootstrap('apache/trunk')
        [node1, node2, node3] = cluster.get_nodes()
        keyspace = cluster.create_ks("test", "REPLICATION = {'class':'SimpleStrategy','replicat_factor':3)")


        valqueryer = ComplexQueryer(cluster, keyspace, "stresstestcf")
        endstate = valqueryer.generate(10000, 0.7, 0.2, 0.1, 0.0)

        cluster.stress("write n=5000000", [node1,node2], parallel=True)
        valqueryer.query([node1,node2,node3], "QUORUM")

        validator = Validator(cluster)
        (correct, percent) = validator.validate(endstate, [node1,node2,node3], "ALL")

        self.assertTrue(correct)


    def node_manipulation(self, cluster):
        
        [node1,node2,node3] = cluster.get_nodes()
        time.sleep(50)
        node1.stop()
        time.sleep(50)
        node1.start()


    def long_validation_test(self):
        cluster = ValCluster(get_ctool_nodes())
        cluster.bootstrap('apache/trunk')
        [node1, node2, node3] = cluster.get_nodes()
        keyspace = cluster.create_ks("test", "REPLICATION = {'class':'SimpleStrategy','replicat_factor':3)")


        valqueryer = ComplexQueryer(cluster, keyspace, "longtestcf")
        endstate = valqueryer.generate(100000, 0.7, 0.2, 0.1, 0.0)

        cluster.stress("write n=50000000", [node1,node2], parallel=True)
        
        p = Process(target=self.node_manipulation, args=(cluster,))

        p.start()
        valqueryer.query([node1,node2,node3], "QUORUM")

        validator = Validator(cluster)
        (correct, percent) = validator.validate(endstate, [node1,node2,node3], "ALL")

        self.assertTrue(correct)
