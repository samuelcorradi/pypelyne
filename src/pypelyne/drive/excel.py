from . import Base
from storagy.conn.excel import Conn as ExcelConn
from memdb.dataset import Dataset


class Drive(Base):

    @staticmethod
    def sources(conn:dict, filter:str=None)->list:
        return ExcelConn.sources(conn['path'], conn['filename'], filter=filter)

    def field_size(self)->dict:
        # implementar para pegar os registros
        # de uma coluna e o tamanho maximo dos
        # valores contidos nela
        pass

    @staticmethod
    def _batch(conn:dict, filter:str=None)->list:
        """
        A forma de carrega a lista de 
        fontes varia de acordo com o
        tipo de fonte.
        """
        batchlst = []
        sources = Drive.sources(conn['path'], conn['filename'], filter)
        for f in sources:
            conn = {'path':conn['path']
                , 'filename':conn['filename']
                # , sheet=self._name
                , 'encode':conn.get('encode', 'utf-8')
                , 'has_header':conn.get('has_header', True)}
            drv = Drive(name=f, conn=conn)
            batchlst.append(drv)

    def _load_conn(self):
        conn = ExcelConn(path=self._params['path']
        , filename=self._params['filename']
        , sheet=self._name
        , encode=self._params.get('encode', 'utf-8')
        , has_header=self._params.get('has_header', True)
        , range=self._params.get('range', None))
        return conn

    def save_dataset(self, dataset:Dataset):
        pass

    def load_dataset(self, filter=[])->Dataset:
        """
        Metodo para carregar o dataset.
        """
        self._dataset = Dataset()
        cols = self._conn.get_col_names()
        for f in cols:
            self._dataset.get_schema().add_field(name=f)
        for r in self._conn.all():
            self._dataset.insert(r)
        return self._dataset