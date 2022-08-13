from pypelyne import Transform as Super

class Transform(Super):

    def __init__(self
        , filter):
        """
        """
        self._filter = filter

    def process(self):
        """
        """
        self._dataset.rewind()
        data = self._dataset.where(self._filter)
        self._dataset = data