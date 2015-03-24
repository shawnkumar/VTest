class Row(object):
    
    def __init__(self, cells):
        self.cells = cells

    def getCells(self):
        return self.cells

    def toString(self):
        for idx, column in enumerate(self.cells):
            if idx == 0:
                rowstring = str(column.get_value())
            else:
                rowstring = rowstring + "|" + str(column.get_value())
        return rowstring