from __future__ import annotations
from memdb.dataset import Dataset
from abc import ABC, abstractmethod
import time
from datetime import datetime
import importlib

class Transform(object):
    """
    Classe abstrata que deve ser herdada
    por todas transformacoes possiveis
    do pacote 'transform'.
    """

    @abstractmethod
    def process(self):
        """
        This method must include processing
        of self._dataset in all subclasses.
        """
        pass

    def exec(self
        , dataset:Dataset
        , copy:bool=True)->Dataset:
        """
        The run method must receive the dataset
        to be processed. You can indicate whether
        changes should be made to a copy, keeping
        the original dataset unchanged.
        """
        self._dataset = dataset.copy(copy_data=True) if copy else dataset
        self.process()
        ds = self._dataset # global to local
        self._dataset = None
        return ds

class Job(object):
    """
    """

    def __init__(self, jobname:str):
        self._jobname = jobname
        self._step = []
        self._log = []
        self._result = []
        
    def add(self, step:Step)->Step:
        return self.add_step(step)

    def get_step(self, name:str)->Step:
        """
        Pega qualquer step dentro de um job pelo nome.
        """
        for step in self._step:
            if step._stepname==name:
                return step
        
    def add_step(self, step:Step)->Step:
        if step.get_job()!=self:
            pass
            # raise Exception("Esta tentando adiconar um step que pertence a outro job.")
        step._inp = self
        self._step.append(step)
        return step

    def dataset(self
        , dataset
        , stepname:str=None
        , copy:bool=False)->Step:
        step = Step(self, stepname)
        step.set_dataset(dataset)
        self.add_step(step)
        return step

    def batch(self
        , drive:str
        , filter:str=None
        , stepname:str=None
        , **kwargs)->Step:
        batchstep = Batch(inp=self
            , drive=drive
            , filter=filter # usado para filtrar a lista de sources
            , stepname=stepname
            , **kwargs)
        self.add_step(batchstep)
        # batch._loadsources()
        return batchstep

    def load(self
        , drive:str
        , name:str
        , stepname:str=None
        , filter=None # os dados a serem carregados da origem podem lah passar por um filtro
        , **kwargs)->Step:
        step = SourceStep(inp=self
            , stepname=stepname
            , drive=drive.lower()
            , name=name
            , filter=filter
            , **kwargs)
        self.add_step(step)
        return step

    def run(self):
        """
        Executa os steps associados a este job.
        Percorre os steps adicionados ao job e
        os executa, colocando o resultado das
        execucoes em uma lista chamada 'result'.
        Alguns steps podem retornar um unico item
        como resultado. Outros podem retornar uma
        lista de resultados (como batch). No caso
        de retornar uma lista de resultados, tudo
        eh colocado junto de forma horizontal numa
        rede final.
        """
        # result = []
        for step in self._step:
            step.process()
        return self._result

class Step(object):
    """
    """

    def __init__(self, inp, stepname:str=None):
        self._inp = inp
        self._stepname = stepname
        self._timer = Timer()
        self._out_error = None
        self.set_bulk_size(1000) # tamanho da pilha de dados lidos
        self._out = []
        self._dataset = None

    def transform(self
        , transform:str
        , stepname:str=None
        , **kwargs:dict)->TransformStep:
        """
        """
        step = TransformStep(inp=self
            , transform=transform
            , stepname=stepname
            , **kwargs)
        self.out(step)
        return step

    def set_bulk_size(self
        , bulk_size:int):
        """
        Define o tamanho do bloco de
        execucao dos registros.
        """
        self._bulk_size = bulk_size
        return self

    def set_dataset(self, dataset):
        """
        Define um dataset para dentro do step.
        """
        self._dataset = dataset
        return self

    def get_dataset(self):
        """
        Retorna o dataset do step.
        """
        return self._dataset

    def get_job(self):
        """
        Retorna o jod do step
        """
        inp = self.get_inp()
        while True:
            if isinstance(inp, Job):
                return inp
            inp = inp.get_inp()
        raise Exception("Falha ao encontrar o job numa cadeia de steps.")

    def get_inp(self):
        return self._inp

    def out(self, step: Step)->Step:
        """
        Conecta o step atual
        a um outro step.
        """
        if not isinstance(step, Step):
            raise Exception("O objeto de saida de um step deve ser tambem do tipo Step.")
        step._inp=self
        self._out.append(step)
        return step

    def save(self
        , drive:str
        , name:str
        , stepname:str=None
        , **kwargs)->Step:
        """
        """
        step = DestStep(inp=self
            , stepname=stepname
            , drive=drive.lower()
            , name=name
            , **kwargs)
        self.out(step)
        return step

    def process(self):
        """
        Todo step deve ser processado.
        """
        #start_time = time.time()
        #self._process()
        #self.exec_time = time.time() - start_time
        self._timer.start()
        self._process()
        self._timer.stop()
        # se nao tiver filho coloca o resultado do processo como resultado do job
        if not self._out:
            self.get_job()._result.append(self)
        for out_step in self._out:
            # out_step.set_dataset(self._dataset)
            out_step.process()
        return self

    def set_error_out(self, step:DestStep):
        """
        """
        self._out_error = step
        return self

    def _process(self):
        """
        Todo step deve ser processado.
        """
        pass

class TransformStep(Step):

    def __init__(self
        , inp
        , transform:str
        , stepname:str=None
        , **kwargs:dict):
        super().__init__(inp, stepname)
        self._transform = TransformStep.load_transform(name=transform, params=kwargs)
        self._params = kwargs

    @staticmethod
    def load_transform(name:str, params:dict)->TransformStep:
        """
        """
        module = importlib.import_module('.transform.{}'.format(name.casefold()), package='package.pyflow')
        my_class = getattr(module, 'Transform')
        my_instance = my_class(**params)
        return my_instance

    def _process(self):
        self._dataset = self._transform.process(self._inp.get_dataset())

    # TRATAMENTO DE ERROS

    #def set_error_log(self, dest:Dest):
    #    self._out_error = dest

    #def _record_error(self, action:str, error_msg:str, data:str=''):
    #    """
    #    Registra um erro ocorrido durante
    #    o processo de etl.
    #    """
    #    if self._out_error is not None:
    #        self._out_error.insert(action, error_msg, data, datetime.now())


class DataStep(Step):
    """
    Classes para aqueles steps que
    envolvem manupilacao dos dados.
    """
    def __init__(self
        , inp
        , drive:str
        , name:str
        , stepname:str=None
        , **kwargs:dict):
        super().__init__(inp=inp, stepname=stepname)
        self._name = name
        self._drive = DataStep.load_drive_inst(drive=drive
            , name=name
            , conn=kwargs)

    @staticmethod
    def load_drive_class(drive:str):
        module = importlib.import_module('.drive.{}'.format(drive.casefold()), package='package.pyflow')
        my_class = getattr(module, 'Drive')
        return my_class

    @staticmethod
    def load_drive_inst(drive:str
        , name:str
        , conn:dict):
        my_class = DataStep.load_drive_class(drive)
        my_instance = my_class(name=name, conn=conn)
        return my_instance

    def get_name(self):
        return self._name

    def rename(self, newname:str):
        self._drive.rename(newname)
        return self

class DestStep(DataStep):
    """
    """
    def _process(self):
        self._dataset = self._inp.get_dataset()
        if not self._dataset.is_empty():
            self._drive.save_dataset(self._dataset)

class SourceStep(DataStep):
    """
    """
    def __init__(self
        , inp
        , drive:str
        , name:str
        , filter=[]
        , stepname:str=None
        , **kwargs:dict):
        super().__init__(inp=inp, drive=drive, name=name, stepname=stepname, **kwargs)
        self._filter = filter

    def _process(self):
        if not self._dataset:
            self._dataset = self._drive.load_dataset(filter=self._filter)


class Batch(Step):
    """
    """
    def __init__(self
        , inp
        , drive:str
        , filter=None # usado para filtrar a lista de sources
        , stepname:str=None
        , **kwargs:dict):
        super().__init__(inp, stepname)
        self._drive = drive
        self._filter = filter
        self._params = kwargs
        self._steps = []

    @staticmethod
    def load_drive_class(drive:str):
        module = importlib.import_module('.drive.{}'.format(drive.casefold()), package='package.pyflow')
        my_class = getattr(module, 'Drive')
        return my_class

    def get_steps(self)->list:
        return self._steps

    def get_step(self, idx:int):
        return self._steps[idx]

    def _add_step(self, step: Step)->Step:
        """
        Adiciona os steps que farao
        parte do conjunto do batch.
        """
        self._steps.append(step)
        return self._steps[-1]
    
    def _loadsources(self):
        """
        A forma de carrega a lista de 
        fontes varia de acordo com o
        tipo de fonte.
        """
        drv_class = Batch.load_drive_class(self._drive)
        drvs = drv_class.sources(conn=self._params, filter=self._filter)
        for name in drvs:
            # o parent (inp) pode ser o step de bach ou o job?
            step = SourceStep(inp=self
                , drive=self._drive
                , name=name
                , filter=[]
                , stepname='optional'
                , **self._params)
            self._add_step(step)
        
    def _process(self):
        """
        Todo step deve ser processado.
        """
        self._loadsources()

    def process(self):
        """
        Todo step deve ser processado.
        """
        self._timer.start()
        self._process()
        self._timer.stop()
        for _, step in enumerate(self._steps):
            for out_step in self._out:
                step.out(out_step)
            step.process()
        return self


class Timer(object):

    def __init__(self):
        """
        """
        self._start = .0
        self._stop = .0

    def start(self):
        """
        """
        self._start = time.time()

    def stop(self):
        """
        """
        self._stop = time.time()

    def total(self):
        """
        """
        if not self._stop:
            self.stop()
        return self._stop - self._start