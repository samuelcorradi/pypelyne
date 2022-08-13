from pypelyne import Transform as Super

class Transform(Super):

    def __init__(self
        , first_line_part:str=""
        , last_line_part:str=""):
        """
        """
        self._first_line_part = first_line_part
        self._last_line_part = last_line_part

    def is_invalid_pos(self, start:int, end:int)->bool:
        """
        """
        return end<start

    def find_first_pos(self)->int:
        """
        Encontra em qual posicao no arquivo a
        primeira linha estah.
        """
        line = self._dataset.first() # le a primeira linha
        while self._dataset.get_index()<self._dataset.len():
            line = ''.join(line)
            if self._is_first_line(line):
                return self._dataset.get_index()
            line = self._dataset.next()
        return -1 # 0

    def find_last_pos(self)->int:
        """
        Encontra em qual posicao dataset
        a string indicada como ultima
        posicao estah.
        """
        line = self._dataset.last() # le a ultima linha
        while self._dataset.get_index()>=0:
            line = ''.join(line)
            if self._is_last_line(line):
                return self._dataset.get_index()
            line = self._dataset.prev()
        return -1 # self._dataset.len() - 1

    def get_merge_pos(self)->tuple:
        """
        Encontra em qual posicao no arquivo a
        primeira linha estah.
        """
        start = self.find_first_pos()
        end = self.find_last_pos()
        if self.is_invalid_pos(start, end):
            raise Exception("Numero final %s menor que o de inicio %s." % (start, end))
        return (start, end)

    def _is_first_line(self, line:str)->bool:
        """
        Isso faz com que get_fist_line() por
        padrao sempre retorne 1. Em arquivos
        com formatos diferentes a primeira linha
        a ser considerada pode estar em outra
        posicao. Entao isso pode ser implementado
        em classes filhas para mudar o comportamento.
        """
        return True if self._first_line_part is None else line.startswith(self._first_line_part)

    def _is_last_line(self, line:str)->bool:
        """
        Por padrao consideramos que a ultima linha
        eh sempre aquela que estiver vazia.
        """
        return True if line.startswith(self._last_line_part) else False
        # return True if not line.strip() else False

    def process(self):
        """
        """
        start, end = self.get_merge_pos()
        print(start, end)
        if start==-1 or end==-1:
            self._dataset.truncate()
            return
        self._dataset.rewind()
        self._dataset.cut(start, end)
