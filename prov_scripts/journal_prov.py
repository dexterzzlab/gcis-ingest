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

def get_doc_prov(j, gcis_url, refList, articleList, personList, organizationList):
    """Generate PROV-ES JSON from GCIS doc metadata."""
    doc = ProvEsDocument()    
    doc_attrs = [
        ("prov:type", 'gcis:Journal'),
        ("prov:label", j['title']),
        ("prov:location", "%s%s"%(gcis_url,j['uri'])),
        ("gcis:online_issn", j['online_issn']),
        ("gcis:print_issn", j['print_issn']),
        ("gcis:publisher", j['publisher']),
        ]

    doc.entity('bibo:%s' % j['identifier'], doc_attrs)

    if j['publisher'] is not None:
        if any(organization['name'] == j['publisher'] for organization in organizationList):
            organization = next(org for org in organizationList if org['name'] == j['publisher'])
            org_attrs = [
                ("prov:type", 'gcis:organization'),
                ("prov:label'", organization['name']),
                ("prov:location", "%s%s"%(gcis_url,organization['uri'])),
                ("gcis:organization_type_identifier", organization['organization_type_identifier']),
                ("gcis:country_code", organization['country_code']),
            ]
            org_id = 'bibo:%s'%organization['identifier']
            doc.entity(org_id, org_attrs)

            doc.governingOrganization(org_id, organization['name'])
    
    prov_json = json.loads(doc.serialize())

    return prov_json

def index_gcis(gcis_url, es_url, index, alias, dump_dir):
    """Index GCIS into PROV-ES ElasticSearch index."""
    conn = get_es_conn(es_url, index, alias)
    refList = get_refList(dump_dir)
    articleList = get_itemList(dump_dir, "article")
    personList = get_itemList(dump_dir, "person")
    organizationList = get_itemList(dump_dir, "organization")
    
    file_path = "%s/journal/"%(dump_dir)
    for (root,dirs,files) in os.walk(file_path):
        for f in files:
            f = "%s%s"%(file_path, f)
            with open(f) as item:
                jsonFile = json.load(item)
                prov = get_doc_prov(jsonFile, gcis_url, refList, articleList, personList, organizationList)
                import_prov(conn, index, alias, prov)

def get_itemList(dump_dir, listType):
    itemList = []
    list_path = "%s/%s"%(dump_dir, listType)
    for (root,dirs,files) in os.walk(list_path):
        for f in files:
            f = "%s/%s"%(list_path, f)
            with open(f) as item:
                item_json = json.load(item)
                itemList.append(item_json)
    return itemList

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
            gcis_url =  "https://gcis-search-stage.jpl.net:3000"
            dt = datetime.utcnow()


            index = "%s-gcis" % app.config['PROVES_ES_PREFIX']
            alias = app.config['PROVES_ES_ALIAS']
            index_gcis(gcis_url, es_url, index, alias, dump_dir)
        else:
            print "Argument provided is not a directory"
    else:
        print "Requires an argument for dump directory"




