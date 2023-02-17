from pyrediscore.redantic import RedisStore, KeyStoreError, StoreKeyNotFound
from .schemas import GODatum, UniprotDatum, SecondaryId, UniprotCollection, UniprotAC
from pyproteinsext import uniprot as pExt
from pydantic import ValidationError
from sys import stderr
from typing import List

class UniprotStore():
    def __init__(self, host:str="127.0.0.1", port:int=6379):
        self.base_store = RedisStore(host, port)
        self.base_store.load_model(UniprotDatum, 'id')
        self.base_store.load_model(SecondaryId, 'id')
        self.base_store.load_model(GODatum, 'id')
        self.base_store.load_model(UniprotCollection, 'comments') # to wipe and replace by 
        

    def wipe_all(self):
        self.base_store.wipe()

    def load_uniprot_xml(self, file=None, stream=None):
        inserted_ok = []
        if not file and not stream:
            raise ValueError("please provide XML source with the file or stream arguments")
        if file:   
            collection = pExt.EntrySet(collectionXML=file)
        else:
            collection = pExt.EntrySet(streamXML=stream)
            
        for prot in collection:
            #print(prot.id, prot.AC)
            #print(prot)
            gos = []
            for go in prot.GO:
                go_obj = GODatum(id = go.id, evidence = go.evidence, term = go.term)
                gos.append(go_obj)
                try: 
                    self.base_store.add(go_obj)
                #    print(go.id, "added")
                except KeyStoreError:
                    #print("Already in db", go.id)
                    pass
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
                    #print(sec_id, "mapping added")
                except KeyStoreError:
                    #print("Already in db", sec_id)
                    pass

            try:
                self.base_store.add(obj)
                #print(prot.id, "added")
            except KeyStoreError:
                #print("Already in db", prot.id)
                pass
            inserted_ok.append(prot.id)
            #print(f"{prot.id} now in db")
        print(f"{len(inserted_ok)} entries added to store")
        return inserted_ok

    def save_collection(self, comments:str, uniprot_ids:List[UniprotAC]):
        coll = UniprotCollection(comments=comments, content=uniprot_ids)
        try:
            self.base_store.add(coll)
            #print(coll.comments, "added")
        except KeyStoreError:
            print("Already in db", coll.comments, file=stderr)
    def delete_collection(self, comments:str):
        try:
            self.base_store.delete(comments, model=UniprotCollection)
        except StoreKeyNotFound:
            print(f"No such collection named {comments}")
            return None
        print(f"Collection \"{comments}\" deleted")
    
    def list_collection(self):
        col_summary = []
        for col_key in self.base_store.list_key(model=UniprotCollection, skip_prefix=True):
            col_data = self.base_store.get(col_key, UniprotCollection)
            col_summary.append( (col_data.comments, col_data.content) )
        return col_summary

    def get_protein_collection(self, collection_id_as_comment):
        try:
            collection = self.base_store.get(collection_id_as_comment, UniprotCollection)
        except StoreKeyNotFound:
            print(f"Collection \"{collection_id_as_comment}\" not found", file=stderr)
            return None
        except KeyError as e:
            print(f"Validation error at key \"{collection_id_as_comment}\": {e}", file=stderr)
        
        for uniprot_id in collection.content:
            try:
                _ = self.get_protein(uniprot_id)
                yield _
            except StoreKeyNotFound:
                print(f"uniprot AC {uniprot_id} not found", file=stderr)
            except KeyError as e:
                print(f"Validation error at key {collection_id_as_comment}: {e}", file=stderr)

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
        except KeyError as e:
            print("Validation error at key {uniprot_id}: {e}", file=stderr)

    def get_proteins_with_mget(self, uniprot_ids):
        pass

    def get_proteins(self, uniprot_ids): 
        resp = {}
        for uniprot_id in uniprot_ids:
            resp[uniprot_id] = self.get_protein(uniprot_id)
        return resp

    def get_collections_from_prots(self, uniprot_ids):
        coll_for_prots = {}
        for coll_name, coll_content in self.list_collection():
            coll_for_prots[coll_name] = len(set(coll_content).intersection(set(uniprot_ids)))
            
        return coll_for_prots
        
    

        