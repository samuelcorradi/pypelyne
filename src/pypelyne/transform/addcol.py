from pypelyne import Transform as Super

class Transform(Super):

    def __init__(self
        , fieldname:str
        , ftype=str
        , size:int=50
        , default=None
        , out_format:str=None
        , primary_key:bool=False
        , auto_increment:bool=False
        , col_ref:int=None
        , pos='a'):
        """
        """
        self._fieldname=fieldname
        self._ftype=ftype
        self._size=size
        self._default=default
        self._out_format = out_format
        self._primary_key=primary_key
        self._auto_increment=auto_increment
        self._col_ref=col_ref
        self._pos=pos

    def process(self):
        """
        """
        print(self._default)
        self._dataset.add_field(name=self._fieldname
            , ftype=self._ftype
            , size=self._size
            , default=self._default
            , out_format = self._out_format
            , primary_key=self._primary_key
            , auto_increment=self._auto_increment
            , col_ref=self._col_ref
            , pos=self._pos)


