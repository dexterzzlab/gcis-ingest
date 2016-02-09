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

    model = requests.get(j['href']).json()
    modelID = 'bibo:%s'%j['identifier']

    
    doc_attrs = [
        ("prov:type", 'gcis:model'),
        ("prov:label", j['name']),
        ("prov:location", "%s%s"%(gcis_url,j['uri'])),
        ("gcis:project_identifier", j['project_identifier']),
        #("prov:wasAttributedTo", contributors),
        ]

    doc.entity('bibo:%s' % j['identifier'], doc_attrs)

    #contributors
    if 'contributors' in model:
        for contributor in model['contributors']:
            if contributor['person_uri'] is not None:
                #print article
                #print contributor['person']

                name  = " ".join([contributor['person'][i] 
                    for i in ('first_name', 'middle_name', 'last_name')
                    if contributor['person'].get(i, None) is not None])
                #print contributor['person_id']
                personAttrs = [
                        ("prov:type", 'gcis:Person'),
                        ("prov:label", "%s"%name),# %s"%(contributor['person']['first_name'],contributor['person']['last_name'])),
                        ("prov:location", "%s%s"%(gcis_url,contributor['person_uri'])),
                        ("gcis:id", str(contributor['person_id'])),
                        ("gcis:orcid", contributor['person']['orcid'])
                        ]
                personID = 'bibo:%s'%contributor['person_id']
                doc.entity(personID, personAttrs)

                doc.wasAssociatedWith(modelID, personID, None, None,{"prov:role": contributor['role_type_identifier']} )
            if contributor['organization'] is not None:
                #make org
                org_attrs = [
                        ("prov:type", "gcis:organization"),
                        ("prov:label", contributor["organization"]["name"]),
                        ("prov:location", "%s%s"%(gcis_url, contributor["organization_uri"])),
                        ("gcis:organization_type_identifier", contributor["organization"]["organization_type_identifier"]),
                        ("gcis:country_code", contributor["organization"]["country_code"]),
                        ]
                orgID = 'bibo:%s'%contributor['organization']['identifier']
                doc.entity(orgID, org_attrs)
                doc.governingOrganization(orgID, contributor['organization']['name'])

   

    #aliases

    prov_json = json.loads(doc.serialize())

    return prov_json

def index_gcis(gcis_url, es_url, index, alias, dump_dir):
    """Index GCIS into PROV-ES ElasticSearch index."""
    conn = get_es_conn(es_url, index, alias)
    refList = get_refList(dump_dir)
    file_path = "%s/model/"%(dump_dir)
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
            gcis_url =  "https://gcis-search-stage.jpl.net:3000"
            dt = datetime.utcnow()


            index = "%s-gcis" % app.config['PROVES_ES_PREFIX']
            alias = app.config['PROVES_ES_ALIAS']
            index_gcis(gcis_url, es_url, index, alias, dump_dir)
        else:
            print "Argument provided is not a directory"
    else:
        print "Requires an argument for dump directory"




