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
    def validate(self, expected_state, nodes, conslevel, non_default_query=None):
        if expected_state.get_num_rows() == 0:
            raise ValueError("Your expected state has no rows")

        keyspace = expected_state.get_keyspace()
        table = expected_state.get_cf()

        if non_default_query==None:
            session = self.cluster.get_session(nodes, keyspace, exclusive=True)
            expected_file = expected_state.get_file()
            self.stats_setup()
            with open(expected_file, 'r') as e:
                for row in e:
                    expected_row = expect_row.rstrip()
                    self.total_expected_results += 1 
                    exp_array = expected_row.split("|")
                    valquery = "SELECT * FROM {cf} WHERE key1 = {k1} and key2 = {k2}".format(cf=table,k1=exp_array[0],k2=exp_array[1])
                    rowresult = session.execute(valquery)
                    self.compareStates(expected_row, rowresult)
                    
        (passes, valid) = self.post_validation()
        return (passes, valid)

    def compareStates(self, expected_row, actual_row):
        expected_array= expected_row.split("|")\

        if len(actual_row) == 0:
            self.unmatched_row(expected_row)
        else:
            for x in range(0, len(expected_array)):
                if expected_array[x] != str(actual[x]):
                    self.unmatched_row(expected_row)
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


