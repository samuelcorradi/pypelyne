import csv
from . import Base
from storagy.conn.csv import Conn as CSVConn
from storagy.conn.directory import Conn as DirectoryConn
from memdb.dataset import Dataset


class Drive(Base):

    @staticmethod
    def sources(conn:dict, filter:str=None)->list:
        return DirectoryConn.sources(path=conn['path'], filter=filter)

    @staticmethod
    def _batch(conn:dict, filter:str=None)->list:
        """
        A forma de carrega a lista de 
        fontes varia de acordo com o
        tipo de fonte.
        """
        batchlst = []
        sources = Drive.sources(conn['path'], filter)
        for f in sources:
            conn = {'path':conn['path'], 'mode':'r', 'encode':'utf-8'}
            drv = Drive(name=f, conn=conn)
            batchlst.append(drv)

    def field_size(self)->dict:
        # implementar para pegar os registros
        # de uma coluna e o tamanho maximo dos
        # valores contidos nela
        pass

    def _load_conn(self):
        conn = CSVConn(path=self._params['path']
        , filename=self._name
        , mode=self._params.get('mode', 'r')
        , encode=self._params.get('encode', 'utf-8')
        , has_header=self._params.get('has_header', True)
        , delimiter=self._params.get('delimiter', ','))
        return conn

    def save_dataset(self, dataset:Dataset):
        h = self._conn.get_handler().get_handler()
        if self._conn._has_header:
            h.writelines("%s\n" % self._conn._delimiter.join(dataset.columns()))
        h.writelines("%s\n" % self._conn._delimiter.join(str(c) for c in line) for line in dataset._data)
        self._conn.disconnect()

    def load_dataset(self, filter=[])->Dataset:
        """
        Metodo para carregar o dataset.
        """
        self._dataset = Dataset()
        row = self._conn.get_handler().get_handler().readline().strip()
        row = csv.reader([row], delimiter=self._conn._delimiter)
        #print(list(row))
        if self._conn._has_header:
            for f in next(row):
                self._dataset.get_schema().add_field(name=f)
            # row = self._conn.get_handler().get_handler().readline().strip()
        else:
            for _ in range(len(next(row))):
                self._dataset.get_schema().add_field()
        print(self._dataset.get_schema().get_all_field_pos())
        while not self._conn.eof():
            row = self._conn.get_handler().get_handler().readline().strip()
            for row in csv.reader([row], delimiter=self._conn._delimiter):
                # print(row)
                if row:
                    self._dataset.insert(row)
        return self._dataset