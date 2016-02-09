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

def get_doc_prov(j, gcis_url, refList, personList, reportList, webpageList):#organizationList, activityList):
    """Generate PROV-ES JSON from GCIS doc metadata."""
    gcis_ns = "http://data.globalchange.gov/gcis.owl#"
    doc = ProvEsDocument()
    
    dataset = requests.get(j['href']).json()
    datasetID = 'bibo:%s' % j['identifier']
    doc_attrs = [
        ("prov:type", 'gcis:Dataset'),
        ("prov:label", j['name']),
        ("prov:location", "%s%s"%(gcis_url, j['uri'])),
        ]
    doc.entity('bibo:%s' % j['identifier'], doc_attrs)

    agent_ids = {}
    org_ids = {}
    #contributors
    if 'contributors' in dataset:
        for contributor in dataset['contributors']:
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
    if dataset['start_time'] is not None:
        start_time = str(dataset['start_time'])
    else:
        start_time = ""
    if dataset['end_time'] is not None:
        end_time = str(dataset['end_time'])
    else:
        end_time = ""
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
    doc.wasGeneratedBy(datasetID, act, end_time, GCIS["%s"%get_uuid("%s:%s"%(datasetID, act_id))])

       
    #aliases

    #instrument measurements

    """
    role_ids = {}
    agent_ids = {}
    org_ids = {}
    personID = None
    #contributors
    if 'contributors' in dataset:
        for contributor in dataset['contributors']:
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
                role_ids[personID] = contributor['role_type_identifier']

                doc.agent(personID, personAttrs)
                agent_ids[personID] = []
                #doc.wasAssociatedWith(datasetID, personID, None, None,{"prov:role": contributor['role_type_identifier']} )
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
    
    if dataset['start_time'] is not None:
        start_time = str(dataset['start_time'])
    else:
        start_time = ""
    if dataset['end_time'] is not None:
        end_time = str(dataset['end_time'])
    else:
        end_time = ""
    #    print j['identifier']
    #else:
    #    start_time = None
    #    end_time = None
    act_id = GCIS["generate-%s"%j['identifier'].replace('/', '-')]
    attrs = []
    for agent_id in agent_ids:
        waw_id = GCIS["%s"%get_uuid("%s:%s"%(act_id, agent_id))]
        doc.wasAssociatedWith(act_id, agent_id, None, waw_id, {'prov:role': GCIS[role_ids[agent_id]] })
        for org_id in agent_ids[agent_id]:
            del_id = GCIS["%s"%get_uuid("%s:%s:%s"%(agent_id, org_id, act_id))]
            doc.delegation(agent_id, org_id, act_id, del_id, {'prov:type':'gcis:worksAt'})
    for org_id in org_ids:
        waw_id = GCIS["%s"%get_uuid("%s:%s"%(act_id, org_id))]
        doc.wasAssociatedWith(act_id, org_id, None, waw_id, {'prov:role':GCIS['Contributor']})
    act = doc.activity(act_id, start_time, end_time, attrs)
    doc.wasGeneratedBy(datasetID, act, end_time, GCIS["%s"%get_uuid("%s:%s"%(datasetID, act_id))])

    """




    #cited by
    if 'cited_by' in dataset:
        for citation in dataset['cited_by']:
            if 'publication' in citation:
                #pub_uri = "%s%s"%(gcis_url, citation['publication'])
                itemType = citation['publication'].split("/")[1]

                itemList = get_itemList(dump_dir, itemType)
                if any(item['uri'] == citation['publication'] for item in itemList):
                    item = next(obj for obj in itemList if obj['uri'] == citation['publication'])
                    item_id = 'bibo:%s'%item['identifier']
                    doc.wasDerivedFrom(item_id, datasetID)

    prov_json = json.loads(doc.serialize())

    return prov_json

def index_gcis(gcis_url, es_url, index, alias, dump_dir):
    """Index GCIS into PROV-ES ElasticSearch index."""
    conn = get_es_conn(es_url, index, alias)
    refList = get_refList(dump_dir)
    personList = get_itemList(dump_dir, "person")
    organizationList = get_itemList(dump_dir, "organization")
    activityList = get_itemList(dump_dir, "activity")
    reportList = get_itemList(dump_dir, "report")
    webpageList = get_itemList(dump_dir, "webpage")
    

    dataset_path = "%s/dataset/"%(dump_dir)
    for (root,dirs,files) in os.walk(dataset_path):
        for f in files:
            f = "%s%s"%(dataset_path, f)
            with open(f) as item:
                dataset = json.load(item)
                prov = get_doc_prov(dataset, gcis_url, refList, personList, reportList, webpageList)# personList, organizationList, activityList)
                import_prov(conn, index, alias, prov)

def get_itemList(dump_dir, listType):
    itemList = []
    listPath = "%s%s"%(dump_dir, listType)
    for (root,dirs,files) in os.walk(listPath):
        for f in files:
            f="%s/%s"%(listPath, f)
            with open(f) as item:
                itemJson = json.load(item)
                itemList.append(itemJson)
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
            gcis_url =  "https://data.globalchange.gov"#"https://gcis-search-stage.jpl.net:3000"
            dt = datetime.utcnow()


            index = "%s-gcis" % app.config['PROVES_ES_PREFIX']
            alias = app.config['PROVES_ES_ALIAS']
            index_gcis(gcis_url, es_url, index, alias, dump_dir)
        else:
            print "Argument provided is not a directory"
    else:
        print "Requires an argument for dump directory"




