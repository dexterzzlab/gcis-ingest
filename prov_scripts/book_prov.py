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

def get_doc_prov(j, gcis_url, refList, organizationList, personList, dump_dir):
    """Generate PROV-ES JSON from GCIS doc metadata."""
    gcis_ns = "http://data.globalchange.gov/gcis.owl#"
    doc = ProvEsDocument(namespaces={ "gcis": gcis_ns, "bibo": "http://purl.org/ontology/bibo/" })
    
#to get people attributed to, you need to grab article ->  jornal_identifier -> look up in references
#    for ref in refList:
#        if ref['child_publication'] == j['uri']:
            
    book = requests.get(j['href']).json()
    bookID = 'bibo:%s'%book['identifier']

    #print j['year']

    doc_attrs = [
        ("prov:type", 'gcis:Book'),
        ("prov:label", j['title']),
        ("prov:location", "%s%s"%(gcis_url, j['uri'])),
        ("gcis:publisher", j['publisher']),
        ("gcis:isbn", j['isbn'])
        ]
    doc.entity('bibo:%s' % j['identifier'], doc_attrs)
    agent_ids = {}
    org_ids = {}

    """
    #connect to publisher
    if j['publisher'] is not None:
        if any(organization['name'] == j['publisher'] for organization in organizationList):
            organization = next(org for org in organizationList if org['name']== j['publisher'])
            org_attrs = [
                    ("prov:type", 'gcis:organization'),
                    ("prov:label", organization['name']),
                    ("prov:location", "%s%s"%(gcis_url, organization['uri'])),
                    ("gcis:organization_type_identifier", organization['organization_type_identifier']),
                    ("gcis:country_code", organization['country_code']),
                    ]
            org_id = 'bibo:%s'%organization['identifier']
            doc.agent(org_id, org_attrs)
#            doc.governingOrganization(org_id, organization['name'])
    """


    #contributors
    if 'contributors' in book:
        for contributor in book['contributors']:
            personID = None 
            if contributor['person_uri'] is not None:
                name  = " ".join([contributor['person'][i] 
                    for i in ('first_name', 'middle_name', 'last_name')
                    if contributor['person'].get(i, None) is not None])
                personAttrs = [
                        ("prov:type", 'gcis:Person'),
                        ("prov:label", "%s"%name),# %s"%(contributor['person']['first_name'],contributor['person']['last_name'])),
                        ("prov:location", "%s%s"%(gcis_url,contributor['person_uri'])),
                        ("gcis:id", str(contributor['person_id'])),
                        ("gcis:orcid", contributor['person']['orcid'])
                        ]
                personID = 'bibo:%s'%contributor['person_id']
                agent_ids[personID] = []
                doc.agent(personID, personAttrs) 
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
                #doc.entity(orgID, org_attrs)
                doc.governingOrganization(orgID, contributor['organization']['name'])
                org_ids[orgID] = True
                if personID in agent_ids:
                    agent_ids[personID].append(orgID)

    #create actvity
    if isinstance(j['year'], int):
        start_time = str(j['year'])
        end_time = str(j['year'])
    else:
        start_time = None
        end_time = None
    act_id = GCIS["generate-%s"%j['identifier'].replace('/', '-')]
    attrs = []
    for agent_id in agent_ids:
        waw_id = GCIS["%s"%get_uuid("%s:%s"%(act_id, agent_id))]
        doc.wasAssociatedWith(act_id, agent_id, None, waw_id, {'prov:role':GCIS['Author']})
        for org_id in agent_ids[agent_id]:
            del_id = GCIS["%s"%get_uuid("%s:%s:%s"%(agent_id, org_id, act_id))]
            doc.delegation(agent_id, org_id, act_id, del_id, {'prov:type':'gcis:worksAt'})
    for org_id in org_ids:
        waw_id = GCIS["%s"%get_uuid("%s:%s"%(act_id, org_id))]
        doc.wasAssociatedWith(act_id, org_id, None, waw_id, {'prov:role':GCIS['Contributor']})
    act = doc.activity(act_id, start_time, end_time, attrs)
    doc.wasGeneratedBy(bookID, act, end_time, GCIS["%s"%get_uuid("%s:%s"%(bookID, act_id))])

   
    


    
    """
    #connect to publisher
    if j['publisher'] is not None:
        if any(organization['name'] == j['publisher'] for organization in organizationList):
            organization = next(org for org in organizationList if org['name']== j['publisher'])
            org_attrs = [
                ("prov:type", 'gcis:organization'),
                ("prov:label", organization['name']),
                ("prov:location", "%s%s"%(gcis_url, organization['uri'])),
                ("gcis:organization_type_identifier", organization['organization_type_identifier']),
                ("gcis:country_code", organization['country_code']),
            ]
            org_id = 'bibo:%s'%organization['identifier']
            doc.agent(org_id, org_attrs)
            doc.governingOrganization(org_id, organization['name'])


    agent_ids = {}
    org_ids = {}
    #role_ids = {}
    if 'contributors' in book:
        for contributor in book['contributors']:
            personID = None
            if contributor['person_uri'] is not None:
                name = " ".join([contributor['person'][i]
                    for i in ('first_name', 'middle_name', 'last_name')
                    if contributor['person'].get(i, None) is not None])
                personAttrs = [
                    ("prov:type", "gcis:Person"),
                    ("prov:label", "%s"%name),
                    ("prov:location", "%s%s"%(gcis_url, contributor['person_uri'])),
                    ("gcis:orcid", contributor['person']['orcid']),
                        ]
                personID = 'bibo:%s'%contributor['person_id']
                agent_ids[personID] = []
                doc.agent(personID, personAttrs)
                #role_ids[personID] = GCIS[contributor['role_type_identifier']] 
            if contributor['organization'] is not None:
                org_attrs = [ 
                        ("prov:type", "gcis:organization"),
                        ("prov:label", contributor["organization"]["name"]),
                        ("prov:location", "%s%s"%(gcis_url, contributor["organization_uri"])),
                        ("gcis:organization_type_identifier", contributor["organization"]["organization_type_identifier"]),
                        ("gcis:country_code", contributor["organization"]["country_code"]),
                ]   
                orgID = 'bibo:%s'%contributor['organization']['identifier']
                doc.agent(orgID, org_attrs)
                doc.governingOrganization(orgID, contributor['organization']['name'])
                if personID in agent_ids:
                    agent_ids[personID].append(orgID)
                    
    #create actvity
    #if isinstance(j['year'], int):
    if j['year'] is not None:
        start_time = str(j['year'])
        end_time = str(j['year'])
    else:
        start_time = ""
        end_time = ""
    #    print j['identifier']
    #else:
    #    start_time = None
    #    end_time = None
    act_id = GCIS["generate-%s"%j['identifier'].replace('/', '-')]
    attrs = []
    for agent_id in agent_ids:
        waw_id = GCIS["%s"%get_uuid("%s:%s"%(act_id, agent_id))]
        doc.wasAssociatedWith(act_id, agent_id, None, waw_id, {'prov:role': role_ids[agent_id] })
        for org_id in agent_ids[agent_id]:
            del_id = GCIS["%s"%get_uuid("%s:%s:%s"%(agent_id, org_id, act_id))]
            doc.delegation(agent_id, org_id, act_id, del_id, {'prov:type':'gcis:worksAt'})
    for org_id in org_ids:
        waw_id = GCIS["%s"%get_uuid("%s:%s"%(act_id, org_id))]
        doc.wasAssociatedWith(act_id, org_id, None, waw_id, {'prov:role':GCIS['Contributor']})
    act = doc.activity(act_id, start_time, end_time, attrs)
    doc.wasGeneratedBy(bookID, act, end_time, GCIS["%s"%get_uuid("%s:%s"%(bookID, act_id))])

    """


    #cited by
    if 'cited_by' in book:
        for citation in book['cited_by']:
            if 'publication' in citation:
                itemType = citation['publication'].split("/")[1]
                itemList = get_itemList(dump_dir, itemType)
                if any(item['uri'] == citation['publication'] for item in itemList):
                    item = next(obj for obj in itemList if obj['uri'] == citation['publication'])
                    itemID = 'bibo:%s'%item['identifier']
                    doc.wasDerivedFrom(itemID, bookID)

    prov_json = json.loads(doc.serialize())

    return prov_json

def get_itemList(dump_dir, listType):
    itemList =[]
    listPath = "%s%s"%(dump_dir, listType)
    for (root, dirs, files) in os.walk(listPath):
        for f in files:
            f= "%s/%s"%(listPath, f)
            with open(f) as item:
                itemJson = json.load(item)
                itemList.append(itemJson)
    return itemList

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
    organizationList = get_itemList(dump_dir, "organization")
    personList = get_itemList(dump_dir, "person")


    for (root,dirs,files) in os.walk(book_path):
        for f in files:
            f = "%s%s"%(book_path, f)
            with open(f) as item:
                book = json.load(item)
                prov = get_doc_prov(book, gcis_url, refList, organizationList, personList, dump_dir)
                import_prov(conn, index, alias, prov)

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




