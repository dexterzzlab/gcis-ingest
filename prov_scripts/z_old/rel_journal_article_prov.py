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
    gcis_ns = "https://gcis-search-stage.jpl.net:3000/gcis.owl#"
    doc = ProvEsDocument()
    bndl = None
    
#to get people attributed to, you need to grab article ->  jornal_identifier -> look up in references
#    for ref in refList:
#        if ref['child_publication'] == j['uri']:
            


    doc_attrs = [
        ("prov:type", 'gcis:Article'),
        ("prov:label", j['title']),
        ("prov:location", j['uri']),
        #("prov:wasAttributedTo", j['']),
        ]
    doc.entity('bibo:%s' % j['identifier'], doc_attrs)

    prov_json = json.loads(doc.serialize())

    return prov_json

def index_gcis(gcis_url, es_url, index, alias, dump_dir):
    """Index GCIS into PROV-ES ElasticSearch index."""
    conn = get_es_conn(es_url, index, alias)
    refList = get_itemList(dump_diri, "reference")
    art_path = "%s/article/"%(dump_dir)
    for (root,dirs,files) in os.walk(art_path):
        for f in files:
            f = "%s%s"%(art_path, f)
            print("f: %s" % f)
            with open(f) as item:
                article = json.load(item)
                prov = get_doc_prov(article, gcis_url, refList)
                print("prov: %s" % json.dumps(prov, indent=2))
                import_prov(conn, index, alias, prov)

def get_itemList(dump_dir, item):
    refList = []
    ref_path = "%s/%s"%(dump_dir,item)
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
            gcis_url =  "https://gcis-search-stage.jpl.net:3000"
            dt = datetime.utcnow()

            journalArtList = get_itemList(dump_dir, "article")

            journalCount = 0
            reportCount = 0
            bookCount = 0
            editedBookCount = 0
            conferenceProceedingsCount = 0
            webpageCount = 0
            bookSectionCount = 0
            electronicArticleCount = 0
            governmentDocCount = 0
            magArtCount = 0
            personalCommCount = 0
            FoBCount = 0
            newsArtCount = 0
            genericCount = 0
            thesisCount = 0
            confPaperCount = 0
            legalCount = 0


            otherList = []
            refList = get_itemList(dump_dir, "reference")
            for item in refList:
                refType = item['attrs']['reftype']
                
                if refType == "Journal Article":
                    journalCount += 1
                    if any(article['doi'] == item['attrs']['DOI'] for article in journalArtList):
                        print True
                        print article['doi'], item['attrs']['DOI']
                    else:
                        print False
                        print article['doi'], item['attrs']['DOI']
                        print article

                elif refType == "Report":
                    reportCount += 1
                elif refType == "Book":
                    bookCount += 1
                elif refType == "Edited Book":
                    editedBookCount += 1
                elif refType == "Conference Proceedings":
                    conferenceProceedingsCount += 1
                elif refType == "Web Page":
                    webpageCount += 1
                elif refType == "Book Section":
                    bookSectionCount += 1
                elif refType == "Electronic Article":
                    electronicArticleCount += 1
                elif refType == "Government Document":
                    governmentDocCount += 1
                elif refType == "Magazine Article":
                    magArtCount += 1
                elif refType == "Personal Communication":
                    personalCommCount += 1
                elif refType == "Film or Broadcast":
                    FoBCount += 1
                elif refType == "Newspaper Article":
                    newsArtCount += 1
                elif refType == "Generic":
                    genericCount += 1
                elif refType == "Thesis":
                    thesisCount += 1
                elif refType == "Conference Paper":
                    confPaperCount += 1
                elif refType == "Legal Rule or Regulation":
                    legalCount += 1
                else:
                    if refType not in otherList:
                        otherList.append(refType)
            print "journal", journalCount
            print "report",reportCount
            print "Book",bookCount
            print "editedBook",editedBookCount
            print "ConcerenceProceedigns",conferenceProceedingsCount
            print "Webpage",webpageCount
            print "Book Section", bookSectionCount
            print "Electronic Aritcle", electronicArticleCount
            print "Govt Doc", governmentDocCount
            print "Mag Art", magArtCount
            print "Personal Communication", personalCommCount
            print "Film or Broadcast", FoBCount
            print "Newspaper Article", newsArtCount
            print "Generic", genericCount
            print "Thesis", thesisCount
            print "Conference Paper", confPaperCount
            print "Legal Rule or Regulation", legalCount
            

            print otherList
            print len(otherList) 
                    #otherCount += 1
                #print item['attrs']['reftype']
#            index = "%s-gcis" % app.config['PROVES_ES_PREFIX']
#            alias = app.config['PROVES_ES_ALIAS']
#            index_gcis(gcis_url, es_url, index, alias, dump_dir)
        else:
            print "Argument provided is not a directory"
    else:
        print "Requires an argument for dump directory"




