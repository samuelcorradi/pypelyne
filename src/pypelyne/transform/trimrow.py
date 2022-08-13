from pypelyne import Transform as Super

class Transform(Super):
    """
    Transformacao para remover
    os espacos do inicio e final
    da string. Recebe o(s) nomes(s)
    das colunas que devem ter os espacos
    removidos. Se nao for passado a lista
    de colunas, todas terao os espacos
    removidos.
    """
    
    def __init__(self
        , cols=None):
        """
        """
        self._cols = cols

    def process(self):
        """
        """
        cols = []
        if not self._cols:
            cols = self._dataset.get_schema().get_all_field_pos().values()
        elif type(self._cols) is str:
            try:
                cols.append(self._dataset.get_schema().get_field_pos(self._cols))
            except:
                pass
        print(cols)
        try:
            for i in cols:
                for row in self._dataset._data:
                    row[i] = row[i].strip()
        except:
            pass

