import uuid
from random import randint, uniform
from row import Row
from cell import Cell
from state import State

class ComplexQueryer(object):

    def __init__(self, session):
        self.session = session
        self.planfile = str(uuid.uuid4())
        self.schema = "CREATE TABLE complextable(key1 int, key2 int, c2 text, c3 double, PRIMARY KEY(key1, key2));"
        self.mutable = []
        self.state = State()
        self.intcount = 0
        session.execute(self.schema)

    def generate(self, num_ops, prob_insert, prob_update, prob_delete, prob_reupdate):
        # for op in num_ops, choose with probabilties one of the operations.
        # if insert - we check to see if there are at least 50 entries in editable set
        # if true then insert immutably into state (call addrow), if false then insert into editable set variable and state
        opsleft = True
        for x in range(0, num_ops):
            op_index = self.choose_op(prob_insert,prob_update,prob_delete)
            print self.mutable
            if op_index == 0 or num_ops-x > 5: #insert
                (query, row, data) = self.insert()
                if len(self.mutable) < 50:
                    self.add_to_plan(query)
                    self.mutable.append((data,row))
                else:
                    self.add_to_plan(query)
                    self.add_to_state(row)
            elif op_index == 1: #update
                (data, row) = self.get_mutable_row()
                (query, newrow, newdata) = self.update(data, row)
                self.add_to_plan(query)
                if uniform(0,1) < prob_reupdate:
                    self.mutable.append((newdata,newrow))
                else:
                    self.add_to_state(newrow)
            elif op_index == 2: #delete
                (data, row) = self.get_mutable_row()
                query = self.delete(data, row)
                self.add_to_plan(query)
        # at end add everything in mutable to state
        for record in self.mutable:
            (query, row) = record
            self.add_to_state(row)

        return self.state

    def choose_op(self, prob_insert, prob_update, prob_delete):
        probs = [prob_insert, prob_update, prob_delete]
        op_prob = uniform(0.00, 1.0)
        for idx, prob in enumerate(probs):
            op_prob = op_prob - prob
            if op_prob <= 0.00:
                return idx

    def add_to_plan(self, query):
        with open(self.planfile, 'a') as f:
            f.write(query + '\n')

    def add_to_state(self, row):
        self.state.add_row(row)

    def get_mutable_row(self):
        index = randint(0, len(self.mutable)-1)
        print len(self.mutable)
        print index

        row = self.mutable[index]
        self.mutable.pop(index)
        return row

    def generate_row_data(self, only_columns):
        c3 = str(uuid.uuid4()).replace('-', '')
        c4 = round(uniform(0.0, 1000000.0), 1)
        print c4
        if only_columns:
            return [c3, c4]
        else:
            c1 = self.intcount
            self.intcount += 1
            c2 = randint(0, 50)
            return [c1, c2, c3, c4]

    def query(self):
        with open(self.planfile, 'r') as plan:
            for query in plan:
                print query.rstrip()
                self.session.execute(query.rstrip())
        #go through everything in plan and query collecting errors (TODO)
        #but for perf reasons - we will have to read and load in memory concurrently TODO!

    def insert(self):
        data = self.generate_row_data(False)
        k1 = data[0]
        k2 = data[1]
        c2 = data[2]
        c3 = data[3]
        query = "INSERT INTO complextable(key1, key2, c2, c3) values(" + str(k1) + "," + str(k2) + ",'" + str(c2) + "'," + str(c3) + ");"
        cells = []
        for col in data:
            cell = Cell(col)
            cells.append(cell)
        row = Row(cells)
        return (query, row, data)

    def update(self, data, row):
        newdata = self.generate_row_data(True)
        k1 = data[0]
        k2 = data[1]
        c2 = newdata[0]
        c3 = newdata[1]

        cells = []
        newdata = [k1, k2, c2, c3]
        for col in newdata:
            cell = Cell(col)
            cells.append(cell)
        newrow = Row(cells)
        query = "UPDATE complextable SET c2 ='" + str(c2) + "',c3=" + str(c3) + " WHERE key1=" + str(k1) + " AND key2=" + str(k2) + ";"
        return (query, newrow, newdata)

    def delete(self, data, row):
        k1 = data[0]
        k2 = data[1]
        query = "DELETE * FROM complextable where key1= " + k1 + " AND key2=" + k2 +";"
        return query