from cluster import ValCluster
from node import ValNode
from utils import *
from complexqueryer import *
from unittest import assertTrue

class TestValidation():

    def stress_validation_test(self):
        cluster = ValCluster(get_ctool_nodes())
        [node1, node2, node3] = cluster.get_nodes()
        cluster.bootstrap('apache/trunk')
        session = cluster.get_session()
        session.execute("CREATE KEYSPACE space WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};")
        session.execute("use space;")

        valqueryer = ComplexQueryer(session)
        endstate = valqueryer.generate(10000, 0.7, 0.2, 0.1, 0.0)

        cluster.stress("write n=5000000", [node1,node2], parallel=True)
        valqueryer.query()

        validator = Validator(session)
        (correct, percent) = validator.validate(endstate, "SELECT * FROM space.complextable LIMIT 10000;")
        self.assertTrue(correct)
