from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn
from pydantic import BaseModel
from typing import List

from .store import UniprotStore

from .store.schemas import UniprotAC

class UniprotRequest(BaseModel) : 
    uniprotIDs : List[UniprotAC]

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

store = UniprotStore()

@app.get("/handshake", response_class=HTMLResponse)
async def handshake(request: Request):
    return 'hello'

@app.get('/uniprot/list')
async def list_protein():
    return store.proteins

@app.get('/uniprot/length')
async def len_db():
    return {"proteins": len(list(store.proteins)), "go_terms" : len(list(store.go_terms))}

@app.get('/uniprot/{uniprot_id}')
async def get_protein(uniprot_id: UniprotAC):
    return store.get_protein(uniprot_id)

@app.post('/uniprots')
async def get_proteins(uniprot_request : UniprotRequest):
    print("get proteins", uniprot_request.uniprotIDs)
    return store.get_proteins(uniprot_request.uniprotIDs)

@app.post('/collection_scan')
async def get_collection(uniprot_request : UniprotRequest):
    print(f"get collection for {len(uniprot_request.uniprotIDs)} proteins")
    return store.get_collections_from_prots(uniprot_request.uniprotIDs)

def start(host, port):
    """Launched with `poetry run start` at root level"""
    uvicorn.run("uniprot_redis.server:app", host=host, port=port, reload=True)

def load_data(xml, coll_name:str):
    coll = store.load_uniprot_xml(xml)
    print(f"Indexing these entries under collection \"{coll_name}\"")
    store.save_collection(coll_name, coll)

def wipe():
    store.wipe_all()