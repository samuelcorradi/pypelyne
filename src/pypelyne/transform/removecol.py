from pypelyne import Transform as Super

class Transform(Super):

    def __init__(self
        , col):
        """
        """
        self._col = col

    def process(self):
        """
        """
        if type(self._col) is list:
            for col in self._col:
                self._dataset.remove_col(col)
        else:
            self._dataset.remove_col(self._col)
