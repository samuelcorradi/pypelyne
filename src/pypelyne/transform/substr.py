from pypelyne import Transform as Super

class Transform(Super):

    def __init__(self
        , colname:str
        , begin
        , end=None
        , new_col=False):
        """
        """
        self._colname = colname
        self._begin = begin
        self._end = end
        self._new_col = new_col
        
    def process(self):
        """
        """
        colpos = self._dataset.get_schema().get_field_pos(self._colname)
        line = self._dataset.first()
        while line:
            # string = line[colpos]
            begin = self._begin
            end = self._end
            if type(begin) is str:
                pos = line[colpos].find(begin)
                # se nao achou o caracter para inicio do corte, pula essa linha
                if pos==-1:
                    line[colpos] = ''
                    line = self._dataset.next()
                    continue
                begin = pos + len(begin)
            if end is None:
                end = len(line[colpos])
            line[colpos] = line[colpos][begin:end]
            line = self._dataset.next()
