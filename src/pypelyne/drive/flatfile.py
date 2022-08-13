from . import Base
from storagy.conn.flatfile import Conn as FlatFileConn
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
        # implementar para pegar a quantidade
        # de linhas e o tamanho delas
        pass

    def _load_conn(self):
        conn = FlatFileConn(path=self._params['path']
            , filename=self._name
            , mode=self._params.get('mode', 'r')
            , encode=self._params.get('encode', 'utf-8'))
        return conn

    def load_dataset(self, filter=[]):
        """
        Metodo para carregar o dataset.
        """
        self._dataset = Dataset()
        self._dataset.get_schema().add_field()
        while not self._conn.eof():
            row = self._conn.get_handler().readline().strip()
            self._dataset.insert([row])
        return self._dataset

    def save_dataset(self, dataset):
        h = self._conn.get_handler()
        for line in dataset._data:
            h.writelines("%s\n" % ''.join([str(r) for r in line]))
        self._conn.disconnect()