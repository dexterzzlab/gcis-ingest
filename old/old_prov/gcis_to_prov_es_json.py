import os, sys, json, requests, types, re, copy
from datetime import datetime
import requests_cache
sys.path.append('/home/ubuntu/facetview-prov-es')

from fv_prov_es import create_app
from fv_prov_es.lib.import_utils import get_es_conn, import_prov

from prov_es.model import (get_uuid, ProvEsDocument, GCIS, PROV, PROV_TYPE, PROV_ROLE, PROV_LABEL, PROV_LOCATION, HYSDS)

requests_cache.install_cache('gcis-import')

doc = ProvEsDocument()

jsonFile = sys.argv[1]

#dirCount = 0
#topDirectory = "PROV_ES_" + str(datetime.date.today())+"_v"
topDirectory = sys.argv[2]
#while os.path.isdir(topDirectory + str(dirCount)):
#    dirCount += 1

#topDirectory = topDirectory + str(dirCount)
if "/" in  jsonFile:
    #open json file and store as json_item
    jsonFileName = jsonFile.split("/")[-1]
    jsonType = jsonFileName.split("_")[0]
else:
    jsonType = jsonFile.split("_")[0]

directory = topDirectory + "/" + jsonType + "/"

if not os.path.isdir(directory):
    os.makedirs(directory)




with open(jsonFile) as item:
    jsonItem = json.load(item)

#build attributes
typeTuple = (PROV_TYPE, GCIS[jsonType])
if jsonType == "person":
    itemID = GCIS["%s" % jsonItem['id']]
    labelTuple = (PROV_LABEL, jsonItem['id'])
else:
    itemID = GCIS["%s" % jsonItem['identifier']]
    labelTuple = (PROV_LABEL, jsonItem['identifier'])
locationTuple = (PROV_LOCATION, jsonItem['href'])#"%s%s" % (gcis_url, parent['url'])))

attributes = [typeTuple, labelTuple, locationTuple]

doc.entity(itemID, attributes)

# serialize
prov_json = json.loads(doc.serialize())

filePath = directory + jsonFileName

filePath = filePath[:-5]

filePath = filePath + ".prov_es.json"


with open(str(filePath), 'w') as fileOutput:
    fileOutput.write(json.dumps(prov_json, sort_keys=True, indent=4, separators=(',', ': ')))
