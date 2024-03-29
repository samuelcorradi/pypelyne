import re
from pypelyne.drive import Base
from pyodbc import ProgrammingError, DataError
from storagy.conn.sqlserver import Conn as SQLConn
from memdb.dataset import Dataset
from pypelyne.exceptions import NoDestFieldError, TruncateDatabaseFieldError

class Drive(Base):

    # def __init__(self, name:str, host:str, db:str, user:str, pwd:str):
    #self._conn.connect()

    @staticmethod
    def sources(conn:dict)->list:
        # return SnowConn.sources(path=conn['path'])
        raise Exception("Metodo source para SQL Server nao implementado.")

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

    def __del__(self):
        self._conn = None

    def _adjust_size(self, dataset:Dataset):
        dest_field_size = self.field_size()
        dset_field_size = dataset.get_fields_size()
        #print(dest_field_size)
        #print(dset_field_size)
        commom_keys = list(set(dest_field_size.keys()) & set(dset_field_size.keys()))
        for k in commom_keys:
            if type(dest_field_size[k]) is int\
                and dest_field_size[k]>0\
                and dest_field_size[k]<dset_field_size[k]:
                self._conn.resize(fieldname=k, size=dset_field_size[k])

    def _load_conn(self):
        conn = SQLConn(host=self._kwargs['host']
            , db=self._kwargs['db']
            , user=self._kwargs['user']
            , pwd=self._kwargs['pwd']
            , tbname=self._name)
        return conn

    def load_dataset(self, filter=[]):
        self._dataset = Dataset()
        field_list = self._conn.field_list()
        for f in field_list:
            self._dataset.get_schema().add_field(name=f)
        self._conn.open()
        data = self._conn.all()
        for r in data:
            self._dataset.insert(list(r))
        self._conn.close()
        return self._dataset
    
    def field_size(self)->dict:
        """
        Lista de campos e tamanhos
        """
        print(self._conn)
        return self._conn.field_size()

    def save_dataset(self, dataset:Dataset):
        self._adjust_size(dataset) # ajusta o destino ao tamanho da fonte
        dest_field_size = self.field_size()
        dset_field_size = dataset.get_fields_size()
        field_list = dataset.columns()
        commom_keys = list(set(dest_field_size.keys()) & set(field_list))
        print("Lista de campos na origem: {}".format(str(dest_field_size)))
        print(dset_field_size)
        print(field_list)
        print("Commomkeys: {}".format(str(commom_keys)))
        self._conn.open()
        for _row in dataset._data:
            row = dataset._parse_row(_row)
            ndata=dict(zip(field_list, row))
            data = {k: v for k, v in ndata.items() if k in commom_keys}
            try:
                self._conn.insert(data=data)
            except ProgrammingError as e:
                msg = str(e)
                m = re.findall('Invalid column name \'([a-zA-Z0-9\_]+)\'', msg)
                if '42S22' in msg:
                    try:
                        self._conn.insert(data=data)
                    except:
                        raise NoDestFieldError(fieldname=m[0])
            except DataError as e:
                msg = str(e)
                if 'String or binary data would be truncated' in msg:
                    flist = []
                    for fname, size in dest_field_size.items():
                        if fname in field_list\
                            and type(size) is int\
                            and size>0\
                            and size<len(data[fname]):
                            flist.append([fname, size, len(data[fname])])
                    raise TruncateDatabaseFieldError(flist=flist)
            except Exception as e:
                if hasattr(e, 'message'):
                    print(e.message)
                else:
                    print(e)
        self._conn.commit()
        self._conn.close()