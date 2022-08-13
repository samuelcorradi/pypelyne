from pypelyne import Transform as Super
import csv
import copy

class Transform(Super):

    def __init__(self
        , col=0
        , delimiter=','
        , quotechar='"'
        , has_header=False):
        """
        """
        self._col = col
        self._delimiter = delimiter
        self._has_header = has_header
        self._quotechar = quotechar

    def process(self):
        """
        """
        ncols = self._dataset.get_schema().len()
        col_pos = self._dataset.get_schema().get_field_pos(self._col)
        self._dataset.rewind()
        data = self._dataset.copy(copy_data=False)
        line = self._dataset.current() # le uma linha
        if line:
            n_col_pos = col_pos
            for row in csv.reader([line[col_pos]], delimiter=self._delimiter, quotechar=self._quotechar):
                for c in row:
                    if self._has_header is True:
                        data.get_schema().add_field(c.strip(), col_ref=n_col_pos)
                    else:
                        data.get_schema().add_field(col_ref=n_col_pos)
                    n_col_pos += 1
        if self._has_header:
            line = self._dataset.next()
        cutpos = data.get_schema().len() - ncols
        while line:
            for row in csv.reader([line[col_pos]], delimiter=self._delimiter, quotechar=self._quotechar):
                current = copy.copy(line)
                row = row[:cutpos]
                current[col_pos+1:col_pos+1] = row + [None]*(cutpos-len(row))
                data.insert(current)
            line = self._dataset.next()
        self._dataset = data

