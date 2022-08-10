"""Uniprot ressources microservice

Usage:
  uniprot_redis service redis start [--rh=<redis_host> --rp=<redis_port>] [--port=<portNumber>]
  uniprot_redis service redis wipe [--rh=<redis_host> --rp=<redis_port>]
  uniprot_redis service redis add <xmlProteomeFile> [--rh=<redis_host> --rp=<redis_port>]
    
Options:
  -h --help     Show this screen.
  --port=<portNumber>  port for public API [default: 2333]
  --rp=<redis_port>  redis DB TCP port [default: 6379]
  --rh=<redis_host>  redis DB http adress [default: localhost]
  --silent  verbosity
  
"""
from docopt import docopt
from .server import start as uvicorn_start
from .server import load_data, wipe
from .io.parse_xml import parse_xml_uniprot


args = docopt(__doc__)

if args["start"]:
    uvicorn_start(args['--rh'], int(args['--port']))

if args['add']:
    load_data(args['<xmlProteomeFile>'])

if args['wipe']:
    wipe()