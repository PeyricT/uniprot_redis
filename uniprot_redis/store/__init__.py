from pyrediscore.redantic import RedisStore, KeyStoreError, StoreKeyNotFound
from .schemas import GODatum, UniprotDatum, SecondaryId
from pyproteinsext import uniprot as pExt
from pydantic import ValidationError
from sys import stderr

class UniprotStore():
    def __init__(self, host:str="127.0.0.1", port:int=6379):
        self.base_store = RedisStore(host, port)
        self.base_store.load_model(UniprotDatum, 'id')
        self.base_store.load_model(SecondaryId, 'id')
        self.base_store.load_model(GODatum, 'id')

    def wipe_all(self):
        self.base_store.wipe()

    def load_uniprot_xml(self, file=None, stream=None):
        if not file and not stream:
            raise ValueError("please provide XML source with the file or stream arguments")
        if file:   
            collection = pExt.EntrySet(collectionXML=file)
        else:
            collection = pExt.EntrySet(streamXML=stream)
            
        for prot in collection:
            print(prot.id, prot.AC)
            print(prot)
            gos = []
            for go in prot.GO:
                go_obj = GODatum(id = go.id, evidence = go.evidence, term = go.term)
                gos.append(go_obj)
                try: 
                    self.base_store.add(go_obj)
                    print(go.id, "added")
                except KeyStoreError:
                    print("Already in db", go.id)
            try :
                obj = UniprotDatum(id=prot.id, 
                    full_name=prot.fullName, 
                    name=prot.name, 
                    gene_name=prot.geneName,
                    taxid=prot.taxid,
                    sequence=prot.sequence,
                    go = gos)
            except ValidationError as e:
                print(f"Validation failed for {prot.id}: {str(e)}", file=stderr)
                continue
            
            for sec_id in prot.AC:
                correspondance_obj = SecondaryId(id=sec_id, parent_id=prot.id)
                try:
                    self.base_store.add(correspondance_obj)
                    print(sec_id, "mapping added")
                except KeyStoreError:
                    print("Already in db", sec_id)

            try:
                self.base_store.add(obj)
                print(prot.id, "added")
            except KeyStoreError:
                print("Already in db", prot.id)

    @property
    def proteins(self):
        for k in self.base_store.list_key(model=SecondaryId, skip_prefix=True):
            yield k

    @property
    def go_terms(self):
        for k in self.base_store.list_key(model=GODatum, skip_prefix=True):
            yield k

    def get_protein(self, uniprot_id):
        try:
            correspondance_obj = self.base_store.get(uniprot_id, SecondaryId)
            obj = self.base_store.get(correspondance_obj.parent_id, UniprotDatum)
            return obj
        except StoreKeyNotFound:
            return None

        