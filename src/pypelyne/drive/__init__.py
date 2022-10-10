from __future__ import annotations
from abc import ABC, abstractmethod
from memdb.dataset import Dataset
from storagy.exceptions import ConnectionFail

class Base(ABC):

    def __init__(self, name:str, **kwargs):
        self._name = name
        self._kwargs = kwargs
        self._dataset = None
        self.validate = None
        self._conn = None
        try:
            self._conn = self._load_conn()
        except ConnectionFail as e:
            print(e)
        except KeyError as e:
            print("{}: Parameter {} required to connect not found.".format(type(self), e))
        except Exception as e:
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)

    def __del__(self):
        """
        When destroying it must be disconnected.
        """
        del self._dataset
        del self._conn

    def get_name(self):
        return self._name

    @classmethod
    def batch(cls, conn:dict, filter)->list:
        """
        Applies a filter to the name of
        data sources and returns only
        those that match the filter.
        """
        batchlst = cls._batch(conn, filter)
        return batchlst

    @staticmethod
    @abstractmethod
    def _batch(conn:dict, filter)->list:
        """
        Metodo para carregar um conjunto de
        drives associados a fonte enviada.
        Deve ser implementado em cada uma
        das subclasses onde o nome da fonte
        eh usada para carregar a lista de
        drives.
        """
        pass

    def get_conn(self):
        return self._conn

    def rename(self, newname:str):
        self._conn.rename(newname)
        return self

    @abstractmethod
    def _load_conn(self):
        pass

    @abstractmethod
    def field_size(self)->dict:
        pass

    @abstractmethod
    def save_dataset(self, dataset:Dataset):
        """
        Salva o dataset.
        """
        if self.validate:
            # TODO: validar se os campos do dataset existem no destino
            # TODO: validar se o tamanho dos dados inseridos batem com o destino
            pass
        pass