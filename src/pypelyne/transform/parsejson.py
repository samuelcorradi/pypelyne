from pypelyne import Transform as Super
import json

def share_keys(dict_list:list, default='')->list:
    """
    Essa funcao recebe uma lista
    de dicionarios. Ele deve fazer
    com que todos dicionarios da lista
    tenham as mesmas chaves.
    """
    key_list = all_keys(dict_list)
    for i in range(len(dict_list)):
        dict_list[i] = merge({}.fromkeys(key_list, default), dict_list[i])
    return dict_list

def all_keys(dict_list:list)->list:
    """
    Recebe uma lista de dicionarios
    e retorna uma lista com um
    conjunto unico de todas chaves
    encontradas em todos dicionarios.
    Codigo anterior abaixo:
    keys = set([])
    for _dict in dict_list:
        keys = set(_dict.keys()) | keys
    return list(keys)
    """
    keys = set(dict_list[0])
    for _dict in dict_list:
        keys.update(list(_dict))
    return list(keys)

def flatten(b, delim:str='__'):
   """
   Funcao para nivelar uma estrutura.
   https://stackoverflow.com/questions/1871524/how-can-i-convert-json-to-csv
   """
   val = {}
   for i in b.keys():
      if isinstance(b[i], dict):
         get = flatten(b[i], delim)
         for j in get.keys():
            val[i + delim + j] = get[j]
      else:
         val[i] = b[i]
   return val

def flatten_dict_array(dic_list:list, delim:str='__')->list:
   """
   Recebe uma lista de dicionatios
   e retorna uma lista de dicionarios
   nivelados
   """
   for i, v in enumerate(dic_list):
      dic_list[i] = flatten(v, delim)
   return dic_list

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
