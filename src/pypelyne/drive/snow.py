from . import Base
from memdb.dataset import Dataset
from servicepy import Conn
from servicepy import Table

class Drive(Base):

    @staticmethod
    def sources(conn:dict)->list:
        # return SnowConn.sources(path=conn['path'])
        raise Exception("Metodo source para SNOW nao implementado.")

    @staticmethod
    def _batch(conn:dict, filter)->list:
        """
        Metodo para carregar um conjunto de
        drives associados a fonte enviada.
        Deve ser implementado em cada uma
        das subclasses onde o nome da fonte
        eh usada para carregar a lista de
        drives.
        """
        raise Exception("Metodo ainda nao implementado.")

    def _load_conn(self):
        conn = Conn(domain=self._kwargs['company']
            , user=self._kwargs['user']
            , pwd=self._kwargs['pwd']
            , buffer=self._kwargs.get('buffer', 10000))
        tbl = Table(conn=conn
            , tablename=self._name
            , display_value=self._kwargs.get('display_value', []))
        return tbl

    def field_size(self)->dict:
        return self._conn.field_size()
        
    def save_dataset(self, dataset):
        pass

    def load_dataset(self, filter=[])->Dataset:
        """
        Metodo para carregar o dataset.
        O atributo filter eh usado
        aqui para filtrar o que vai ser
        carregado para o dataset.
        """
        self._dataset = Dataset()
        field_list = self._conn.field_list()
        for f in field_list:
            self._dataset.get_schema().add_field(name=f)
        data = self._conn.select(where=filter, orderby=['sys_updated_on']) # limit=limit,
        for r in data:
            row = []
            for f in field_list:
                row.append(r.get(f, None))
            self._dataset.insert(row)
        return self._dataset 

