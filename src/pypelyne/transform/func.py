from pypelyne import Transform as Super

class Transform(Super):

    def __init__(self
        , func
        , **param:dict):
        """
        """
        self._func=func
        self._param = param

    def process(self):
        """
        """
        self._func(self._dataset, **self._param)


