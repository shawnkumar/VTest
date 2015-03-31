import uuid
from cell import Cell
from row import Row
from state import State
from datetime import datetime

class Validator(object):

    def __init__(self, cluster):
        self.cluster = cluster

    def result_to_row(self, rowresult):
        cells = []
        for column in rowresult:
            cells.append(Cell(column))
        return Row(cells)

    #we want validate to:
    # 1) take expected state, consistency level, nodes to validate against, query template?
    # 2) for each row in expected state, query nodes at cl
    # 3) check values in result against expected state
    # 4) log, record, count etc. 
    def validate(self, expected_state, keyspace, table, non_default_query=None, conslevel, nodes):
        if expected_state.get_num_rows() == 0:
            raise ValueError("Your expected state has no rows")

        if non_default_query==None:
            session = self.cluster.get_session(nodes, keyspace, exclusive=True)
            query = "SELECT * FROM {ks}.{cf} WHERE key1 = ? and key2 = ?".format(ks=keyspace, cf=table)
            preparedquery = session.prepare(query)
            expected_file = expected_state.get_file()
            self.stats_setup()
            with open(expected_file, 'r') as e:
                for expect_row in e:
                    expected_row = expect_row.rstrip()
                    self.total_expected_results += 1 
                    expected_array= expected_row.split("|")
                    rowresult = session.execute(preparedquery, expected_array[0:2])
                    
        (passes, valid) = self.post_validation()
        return (passes, valid)

    def compareStates(self, expected_row, actual):
        matches = True
        expected_array= expected_row.split("|")
        for x in range(0, len(expected)):
            if expected[0] != str(actual[0])
                matches = False
                break

    def stats_setup(self):
        self.unmatched_rows_file = 'unmatchedrows' + str(datetime.now()).replace(' ', '-')
        self.total_expected_results = 0
        self.total_unmatched_results = 0

    def post_validation(self):
        expected = "\nTotal Expected Rows: " + str(self.total_expected_results)
        unmatched = "\nTotal Unmatched Rows: " + str(self.total_unmatched_results)
        percent = (float(self.total_unmatched_results)/float(self.total_expected_results))*100.00
        valid = "\nPercentage Data Invalid: " + str(percent)
        with open(self.unmatched_rows_file, 'a') as u:
            u.write(expected)
            u.write(unmatched)
            u.write(valid)
        return (self.total_expected_results == self.total_unmatched_results, valid)

    def unmatched_row(self, expected_row):
        with open(self.unmatched_rows_file, 'a') as u:
            u.write(expected_row +'\n')
        self.total_unmatched_results += 1


