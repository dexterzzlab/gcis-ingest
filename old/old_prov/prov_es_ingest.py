import os, sys, json, requests, types, re, copy
from datetime import datetime
import requests_cache
requests_cache.install_cache('gcis-import')
sys.path.append('/home/ubuntu/facetview-prov-es')
from fv_prov_es import create_app
from fv_prov_es.lib.import_utils import get_es_conn, import_prov

from prov_es.model import (get_uuid, ProvEsDocument, GCIS, PROV, PROV_TYPE,
                                   PROV_ROLE, PROV_LABEL, PROV_LOCATION, HYSDS)




env = os.environ.get('PROVES_ENV', 'prod')
app = create_app('fv_prov_es.settings.%sConfig' % env.capitalize(), env=env)
es_url = app.config['ES_URL']
gcis_url =  "http://data.globalchange.gov"
dt = datetime.utcnow()
                    #index = "%s-%04d.%02d.%02d" % (app.config['PROVES_ES_PREFIX'],
                        #                               dt.year, dt.month, dt.day)
index = "%s-gcis" % app.config['PROVES_ES_PREFIX']
alias = app.config['PROVES_ES_ALIAS']

conn = get_es_conn(es_url, index, alias)

#get json file
#prov = get_image_prov(img_md, gcis_url)
#print(json.dumps(prov, indent=2))
with open(sys.argv[1]) as item:
    prov_es_json = json.load(item)

import_prov(conn, index, alias, prov_es_json)


#index_gcis(gcis_url, es_url, index, alias)
