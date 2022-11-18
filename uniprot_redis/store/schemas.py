import string
from pydantic import BaseModel
from typing import Optional
from uuid import uuid4
import re
from typing import List

uniprot_acc_regex = re.compile(
    r'[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}'
)

class UniprotAC(str):
    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls.validate
    
    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')
        m = uniprot_acc_regex.fullmatch(v.upper())
        if not m:
            raise ValueError(f"invalid uniprot accession at {v}")
        # you could also return a string here which would mean model.post_code
        # would be a string, pydantic won't care but you could end up with some
        # confusion since the value's type won't match the type annotation
        # exactly
        return cls(f'{v.upper()}')

    def __repr__(self):
        return f'{super().__repr__()}'

def generateUUID():
    return uuid4().hex

class GODatum(BaseModel):
    id: str
    evidence: str
    term: str

class UniprotDatum(BaseModel):
    id: UniprotAC
    full_name: str
    name: str
    gene_name: Optional[str]
    taxid: int
    sequence : str
    go : List[GODatum]

class SecondaryId(BaseModel):
    id: UniprotAC
    parent_id: UniprotAC

class UniprotCollection(BaseModel):
    comments:str
    content:List[UniprotAC]
