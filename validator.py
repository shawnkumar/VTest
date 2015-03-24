import uuid
from cell import Cell
from row import Row
from state import State
from datetime import datetime

class Validator(object):

    def __init__(self, session):
        self.session = session

    def result_to_row(self, rowresult):
        cells = []
        for column in rowresult:
            cells.append(Cell(column))
        return Row(cells)

    def validate(self, expected_state, val_query):
        data = self.session.execute(val_query)

        if expected_state.get_num_rows() == 0 or len(data) == 0:
            raise ValueError("Your query and/or expected state has no rows")

        for idx,row in enumerate(data):
            row_object = self.result_to_row(row)
            if idx==0:
                querystate = State([row_object])
            else:
                querystate.add_row(row_object)
        self.compareStates(expected_state, querystate)

    # perhaps can improve perf here by doing sort and just merging?
    def compareStates(self, expected, actual):
        expected_file = expected.get_file()
        self.stats_setup()
        actual_file = actual.get_file()
        with open(expected_file, 'r') as e:
            for expect_row in e:
                expected_row = expect_row.rstrip()
                self.total_expected_results += 1 
                print "expected_row: " + expected_row
                foundmatch = False
                while not foundmatch:
                    with open(actual_file, 'r') as a:
                        for act_row in a:
                            actual_row = act_row.rstrip()
                            print "actual_row: " + actual_row
                            if expected_row == actual_row:
                                foundmatch = True
                                break
                    if not foundmatch:
                        print "notfound:"
                        self.unmatched_row(expected_row)
                        break
        self.post_validation()

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

    def unmatched_row(self, expected_row):
        with open(self.unmatched_rows_file, 'a') as u:
            u.write(expected_row +'\n')
        self.total_unmatched_results += 1


