from pypelyne import Transform as Super
from utils.dict import flatten_dict_array, share_keys
import json

class Transform(Super):

    def __init__(self
        , colname:str):
        """
        """
        self._colname = colname
        
    def process(self):
        """
        """
        colpos = self._dataset.get_schema().get_field_pos(self._colname)
        jrows = []
        line = self._dataset.first() # le uma linha
        while line:
            string = line[colpos]
            j = json.loads(string)
            jrows.append(j)
            line = self._dataset.next()
        jrows = share_keys(flatten_dict_array(jrows))
        fnames = jrows[0].keys()
        for f in fnames:
            self._dataset.add_field(name=f)
            fpos =self._dataset.get_schema().get_field_pos(f)
            for i in range(len(jrows)):
                self._dataset._data[i][fpos] = jrows[i][f]
