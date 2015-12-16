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
    gcis_ns = "http://data.globalchange.gov/gcis.owl#"
    doc = ProvEsDocument(namespaces={ "gcis": gcis_ns, "bibo": "http://purl.org/ontology/bibo/" })
    
#to get people attributed to, you need to grab article ->  jornal_identifier -> look up in references
#    for ref in refList:
#        if ref['child_publication'] == j['uri']:
            


    doc_attrs = [
        ("prov:type", 'gcis:Book'),
        ("prov:label", j['title']),
        ("prov:location", j['uri']),
        #(cito:citedBy, ....search references),
        ("gcis:publisher", j['publisher']),
        ("gcis:isbn", j['isbn'])
        ]

    prov_json = json.loads(doc.serialize())

    return prov_json

def get_refList(dump_dir):
    refList = []
    ref_path = "%s/reference/"%dump_dir
    for (root,dirs,files) in os.walk(ref_path):
        for f in files:
            f = "%s%s"%(ref_path, f)
            with open(f) as item:
                ref = json.load(item)
                refList.append(ref)
    return refList


def index_gcis(gcis_url, es_url, index, alias, dump_dir):
    """Index GCIS into PROV-ES ElasticSearch index."""
    conn = get_es_conn(es_url, index, alias)
    refList = get_refList(dump_dir)
    book_path = "%s/book/"%(dump_dir)
    for (root,dirs,files) in os.walk(book_path):
        for f in files:
            f = "%s%s"%(book_path, f)
            with open(f) as item:
                book = json.load(item)
                prov = get_doc_prov(book, gcis_url, refList)
                import_prov(conn, index, alias, prov)

if __name__ == "__main__":
    if sys.argv[1] is not None:
        if os.path.exists(sys.argv[1]):
            dump_dir = sys.argv[1]
            env = os.environ.get('PROVES_ENV', 'prod')
            app = create_app('fv_prov_es.settings.%sConfig' % env.capitalize(), env=env)
            es_url = app.config['ES_URL']
            gcis_url =  "http://data.globalchange.gov"
            dt = datetime.utcnow()


            index = "%s-gcis" % app.config['PROVES_ES_PREFIX']
            alias = app.config['PROVES_ES_ALIAS']
            index_gcis(gcis_url, es_url, index, alias, dump_dir)
        else:
            print "Argument provided is not a directory"
    else:
        print "Requires an argument for dump directory"




