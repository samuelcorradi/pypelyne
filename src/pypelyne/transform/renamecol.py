from pypelyne import Transform as Super

class Transform(Super):

    def __init__(self
        , rename:dict):
        """
        """
        self._rename = rename

    def process(self):
        """
        """
        for k, v in self._rename.items():
            try:
                self._dataset.get_schema().rename_field(name=k, new_name=v)
            except:
                pass
