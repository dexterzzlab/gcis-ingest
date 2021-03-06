#!/usr/bin/env python
import os, sys, json, requests, types, re, copy
from datetime import datetime
import requests_cache
import json
from fv_prov_es import create_app
from fv_prov_es.lib.import_utils import get_es_conn, import_prov

from prov_es.model import (get_uuid, ProvEsDocument, GCIS, PROV, PROV_TYPE,
                                   PROV_ROLE, PROV_LABEL, PROV_LOCATION, HYSDS)


requests_cache.install_cache('gcis-import')

def get_doc_prov(j, gcis_url, refList):
    """Generate PROV-ES JSON from GCIS doc metadata."""
    doc = ProvEsDocument()
    name  = " ".join(j[i] 
        for i in ('first_name', 'middle_name', 'last_name')
        if j.get(i, None) is not None)
 
    doc_attrs =[("prov:type", 'gcis:Person'),
        ("prov:label", name),#j['first_name']),
        ("prov:location", "%s%s"%(gcis_url,j['uri'])),
        ("gcis:id", j['id']),
        ("gcis:orcid", j["orcid"]),
        #("prov:wasAttributedTo, contributors),
        ]

    doc.agent('bibo:%s' % j['id'], doc_attrs)
    del_id = GCIS["%s"%get_uuid("%s:%s:%s"%(j['id'], None, None))]

    doc.delegation('bibo:%s'%j['id'], None, None, del_id, None)
    #for org_id in agent_ids[agent_id]:
    #   del_id = GCIS["%s"%get_uuid("%s:%s:%s"%(agent_id, org_id, act_id))]
    #doc.delegation(agent_id, org_id, act_id, del_id, {'prov:type':'gcis:worksAt'})


    prov_json = json.loads(doc.serialize())

    return prov_json

def index_gcis(gcis_url, es_url, index, alias, dump_dir):
    """Index GCIS into PROV-ES ElasticSearch index."""
    conn = get_es_conn(es_url, index, alias)
    refList = get_refList(dump_dir)
    file_path = "%s/person/"%(dump_dir)
    for (root,dirs,files) in os.walk(file_path):
        for f in files:
            f = "%s%s"%(file_path, f)
            with open(f) as item:
                jsonFile = json.load(item)
                prov = get_doc_prov(jsonFile, gcis_url, refList)
                import_prov(conn, index, alias, prov)

def get_refList(dump_dir):
    refList = []
    ref_path = "%s/reference"%dump_dir
    for (root,dirs,files) in os.walk(ref_path):
        for f in files:
            f = "%s/%s"%(ref_path, f)
            with open(f) as item:
                ref = json.load(item)
                refList.append(ref)
    return refList

if __name__ == "__main__":
    if sys.argv[1] is not None:
        if os.path.exists(sys.argv[1]):
            dump_dir = sys.argv[1]
            env = os.environ.get('PROVES_ENV', 'prod')
            app = create_app('fv_prov_es.settings.%sConfig' % env.capitalize(), env=env)
            es_url = app.config['ES_URL']
            gcis_url =  "https://data.globalchange.gov"#"https://gcis-search-stage.jpl.net:3000"
            dt = datetime.utcnow()


            index = "%s-gcis" % app.config['PROVES_ES_PREFIX']
            alias = app.config['PROVES_ES_ALIAS']
            index_gcis(gcis_url, es_url, index, alias, dump_dir)
        else:
            print "Argument provided is not a directory"
    else:
        print "Requires an argument for dump directory"




