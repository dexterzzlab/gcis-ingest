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

def get_doc_prov(j, gcis_url, refList, journalList, organizationList, personList, dump_dir):
    """Generate PROV-ES JSON from GCIS doc metadata."""
    gcis_ns = "https://gcis-search-stage.jpl.net:3000/gcis.owl#"
    doc = ProvEsDocument()
    bndl = None
    
    article = requests.get(j['href']).json()

    journalID = None

    #make journal
    if any(journal['identifier'] == article['journal_identifier'] for journal in journalList):
        journal = next(jour for jour in journalList if jour['identifier'] == article['journal_identifier'])
        journalAttrs = [
                ("prov:type", 'gcis:Journal'),
                ("prov:label", journal['title']),
                ("prov:location", "%s%s"%(gcis_url,journal['uri'])),
                ("gcis:online_issn", journal['online_issn']),
                ("gcis:print_issn", journal['print_issn']),
                ("gcis:publisher", journal['publisher']),
                ]
        journalID = 'bibo:%s'%journal['identifier']
        doc.entity(journalID, journalAttrs)

    #get organization/publisher if any
    if journal['publisher'] is not None:
        if any(organization['name'] == journal['publisher'] for organization in organizationList):
            organization = next(org for org in organizationList if org['name'] == journal['publisher'])
            org_attrs = [
                    ("prov:type", 'gcis:organization'),
                    ("prov:label", organization['name']),
                    ("prov:location", "%s%s"%(gcis_url,organization['uri'])),
                    ("gcis:organization_type_identifier", organization['organization_type_identifier']),
                    ("gcis:country_code", organization['country_code']),
                    ]
            org_id = 'bibo:%s'%organization['identifier']
            doc.entity(org_id, org_attrs)

            doc.governingOrganization(org_id, organization['name'])

    #make article
    articleAttrs = [
        ("prov:type", 'gcis:Article'),
        ("prov:label", article['title']),
        ("prov:location","%s%s"%(gcis_url, article['uri'])),
        ("dcterms:isPartOf", journalID),
    ]
    articleID = 'bibo:%s'%article['identifier']
    doc.entity(articleID, articleAttrs)

    #link journal to article
    if journalID is not None:
        doc.hadMember(journalID, articleID)

    agent_ids = {}
    org_ids = {}
    #contributors
    if 'contributors' in article:
        for contributor in article['contributors']:
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
    doc.wasGeneratedBy(articleID, act, end_time, GCIS["%s"%get_uuid("%s:%s"%(articleID, act_id))])


    #cited by?
    if 'cited_by' in article:
        for citation in article['cited_by']:
            if 'publication' in citation:
                #pub_uri = "%s%s"%(gcis_url, citation['publication'])
                itemType = citation['publication'].split("/")[1]
                
                itemList = get_itemList(dump_dir, itemType)
                if any(item['uri'] == citation['publication'] for item in itemList):
                    item = next(obj for obj in itemList if obj['uri'] == citation['publication'])
                    item_id = 'bibo:%s'%item['identifier']
                    doc.wasDerivedFrom(item_id, articleID)
                    print articleID
    prov_json = json.loads(doc.serialize())

    return prov_json

def index_gcis(gcis_url, es_url, index, alias, dump_dir):
    """Index GCIS into PROV-ES ElasticSearch index."""
    conn = get_es_conn(es_url, index, alias)
    refList = get_refList(dump_dir)
    art_path = "%s/article/"%(dump_dir)
    journal_path = "%s/journal/"%dump_dir
    person_path = "%s/person/"%dump_dir
    
    journalList = get_itemList(dump_dir, "journal") 
    personList = get_itemList(dump_dir,"person") 
    organizationList = get_itemList(dump_dir, "organization")

    modPersonList = []
    for person in personList:
        #print str(person['last_name']) + ", " + str(person['first_name']) + " " + str(person['middle_name'])
        #personName = " ".join(person[i] for i in ('first_name', 'middle_name', 'last_name') if person.get(i, None) is not None)
        if person['last_name'] is not None:
            personName = "%s"%person['last_name']
        if person['first_name'] is not None:
            personName = "%s, %s"%(personName, person['first_name'])
        if person['middle_name'] is not None:
            personName = "%s %s"%(personName, person['middle_name'])
        modPersonList.append(personName)
        #print personName

    for (root,dirs,files) in os.walk(art_path):
        for f in files:
            f = "%s%s"%(art_path, f)
            with open(f) as item:
                article = json.load(item)
                prov = get_doc_prov(article, gcis_url, refList, journalList, organizationList, personList, dump_dir)
                import_prov(conn, index, alias, prov)

def get_itemList(dump_dir, listType):
    itemList = []
    listPath = "%s%s"%(dump_dir, listType)
    #print listPath
    for(root,dirs,files) in os.walk(listPath):
        for f in files:
            f = "%s/%s"%(listPath, f)
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
            #img_path = "%s/%s"%(sys.argv[1], "image/")
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




