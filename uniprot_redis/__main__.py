"""Uniprot ressources microservice

Usage:
  uniprot_redis service start [--rh=<redis_host> --rp=<redis_port>] [--port=<portNumber>]
  uniprot_redis service wipe [--rh=<redis_host> --rp=<redis_port>]
  uniprot_redis service add <xmlProteomeFile> [--rh=<redis_host> --rp=<redis_port> --as=<collection_name>]
    
Options:
  -h --help     Show this screen.
  --port=<portNumber>  port for public API [default: 2333]
  --rp=<redis_port>  redis DB TCP port [default: 6379]
  --rh=<redis_host>  redis DB http adress [default: localhost]
  --as=<collection_name>  assign a name to the inserted collection [default: basename of the xml file]
  --silent  verbosity
  
"""
from docopt import docopt
from .server import start as uvicorn_start
from .server import load_data, wipe
from os.path import basename

args = docopt(__doc__)

if args["start"]:
    uvicorn_start(args['--rh'], int(args['--port']))

if args['add']:
    coll_id = None
    if args['--as'] == "basename of the xml file":
        coll_id = basename(args['<xmlProteomeFile>'])
        print(f"Warning: no collection name provided, using {coll_id}")
    else:
        coll_id = args['--as']
    load_data(args['<xmlProteomeFile>'], coll_id)

if args['wipe']:
    wipe()