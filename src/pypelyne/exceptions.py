class NoDestFieldError(Exception):
    message = "Campo nao existe no destino."
    def __init__(self, fieldname):
        super().__init__(NoDestFieldError.message)
        self.fieldname = fieldname

    def __str__(self)->str:
        return NoDestFieldError.message[0:-1] + ": \'" + self.fieldname + '\''

class TruncateDatabaseFieldError(Exception):
    message = "Dados dos campos podem ser truncados."
    def __init__(self, flist:list):
        super().__init__(TruncateDatabaseFieldError.message)
        self.flist = flist

    def __str__(self)->str:
        return TruncateDatabaseFieldError.message[0:-1] + ": \'" + ', '.join(["{}({},{})".format(*f) for f in self.flist]) + '\''