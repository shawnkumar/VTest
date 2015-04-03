from validator import Validator
from cassandra.cluster import Cluster
from cell import Cell
from row import Row
from state import State
from complexqueryer import ComplexQueryer

cluster = Cluster()
session = cluster.connect()


schema = "CREATE KEYSPACE space WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};"
table = "CREATE TABLE tab (key int PRIMARY KEY, val int);"



#session.execute(schema)
session.execute("use space;")

# query_obj = ComplexQueryer(session)

# state = query_obj.generate(100, 0.6, 0.2, 0.2, 0.0)

# query_obj.query()

# val = Validator(session)
# val.validate(state, "SELECT * FROM space.complextable;")

#session.execute(table)

session.execute("insert into tab(key,val) values(1,1);")

query = "select * from tab where key = 2;"

#prepq = session.prepare(query)

result = session.execute(query)



for idx, thing in enumerate(result):
    print "here"
    print thing






# rows = []
# for x in range(0, 100):
#     session.execute("INSERT INTO space.tab(key, val) VALUES(" + str(x) + ',' + str(x) + ");")
#     rows += [Row([Cell(x), Cell(x)])]

# state = State("output.txt", rows)

# val = Validator(session)
# val.validate(state, "SELECT * FROM space.tab limit 50;")