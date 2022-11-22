The aim of uniprot-redis is to have a rapid access to uniprot data previously stored in redis instance, and querying them by uniprot protein ids. 

## Start and query uniprot-redis in a python environment with poetry

### 1. Install and start redis

Install redis following the website instruction : https://redis.io/docs/getting-started/

Then run a redis instance (it needs to run constantly so we recommand to use `screen` or equivalent)
```
screen -S redis
redis-server
```
then `ctrl + A + D` to quit the screen. `screen -r redis` if you want to go back to it. 

Be aware that it will created a local "save file" of the instance (dump.rdb) in the directory you launch the command. You will need to launch redis-server in the directory where dump.rdb is stored if you want to retrieve the data later. 

### 2. Install uniprot-redis
uniprot-redis is available as a pip package
``` 
pip install uniprot-redis
```

### 3. Run uniprot-redis and store informations 
You need to store into redis the proteins you want to. uniprot-redis can load xml proteome file downloadable here : https://www.uniprot.org/proteomes?query=* 

Choose your organisms of interest. For example if you want to have access to all human proteins you will need to download UP000005640 proteome file (https://www.uniprot.org/proteomes/UP000005640)
Download => Download all => Format : XML => Download

* Load a proteome into uniprot-redis

1. Through python
```python
from uniprot_redis.store import UniprotStore
store = UniprotStore()
protein_collection = store.load_uniprot_xml(file=<xml file>)
# You can identify this protein collection by keyword in order to retrieve it later
store.save_collection('my_id', protein_collection)
# List all collections
store.list_collection()
```

### 4. Interrogate uniprot-redis

You can now have an easy access to all stored protein through python code

```python
# Access a protein through its uniprot id
store.get_protein('<uniprot id>')
# Iterator through a collection
store.get_protein_collection('<collection id>')
# Iterator through all proteins stored in the database
store.proteins
```

#### 5. Uniprot store gestion

```python
# Wipe all database
store.wipe_all()
# Delete a collection 
store.delete_collection('<collection id>')
```

See [notebooks/uniprot_redis_demo.ipynb](notebooks/uniprot_redis_demo.ipynb) for usage examples 






