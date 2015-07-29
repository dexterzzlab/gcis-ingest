import os, json, requests, types, re
import requests_cache
from pyes import ES
import sys



#sys.path.append('/home/ubuntu/facetview-gcis')
from gcis import create_app


requests_cache.install_cache('gcis-import')


FIGURES_RE = re.compile(r'\(figure(?:s)?\s+(\d+\.\d+)(?:\s+and\s+(\d+.\d+))?', re.I)
TABLES_RE = re.compile(r'\(table(?:s)?\s+(\d+\.\d+)(?:\s+and\s+(\d+.\d+))?', re.I)
json_type = sys.argv[1]

def get_es_conn(es_url, index):
    conn = ES(es_url)
    if not conn.indices.exists_index(index):
        conn.indices.create_index(index)
    return conn

#Ingest single item
def index_json(gcis_url, es_url, index):
    #establish connection to elastic search
    conn = get_es_conn(es_url, index)

    #open json file and store as json_item
    with open(sys.argv[2]) as item:
        json_item = json.load(item)
    
    #create exception for person, since person does not have item['identifier'] field... has item['id']
    if json_type != "person":
        conn.index(json_item, index, json_type, json_item['identifier'])
    else:
        conn.index(json_item, index, json_type, json_item['id'])


if __name__ == "__main__":
    #Set up environment variables
    env = os.environ.get('GCIS_ENV', 'prod')
    app = create_app('gcis.settings.%sConfig' % env.capitalize(), env=env)
    es_url = app.config['ELASTICSEARCH_URL']
    gcis_url =  app.config['GCIS_REST_URL']
    index = app.config['GCIS_ELASTICSEARCH_INDEX']
    #ingest single item
    index_json(gcis_url, es_url, index)
