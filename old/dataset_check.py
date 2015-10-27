import os, json, requests, re

localList = []
dgcList = []
count = 0

localDir = "/home/gcisops/ops/gcis-crawler/GCIS_DUMP/GCIS_2015-10-07_v0/dataset"
dgcDir = "http://data.globalchange.gov/dataset.json?all=1"

bookJson = requests.get(dgcDir).json()

for block in bookJson:
    identifier = block['href']
    identifier = identifier[:-5]
    identifier = identifier.replace("http://data.globalchange.gov/","")
    #identifier = re.sub(r'\W+', '_', identifier)
    identifier = identifier[8:]
    identifier = "dataset_" + identifier
    identifier = identifier+".json"
    
    print identifier    
    dgcList.append(identifier)
    #dgcList.append(block['identifier'])

if len(dgcList) == len(set(dgcList)):
    print "There are no duplicates"
else:
    print "There is a duplicate"
print len(set(dgcList))



duplicates = []

for i, elem in enumerate(dgcList):
    if i != 0:
        if elem==old:
            duplicates.append(elem)
            old = None
            continue
    old = elem

print duplicates


#for x in dgcList:
#    print x

#for (root, dirs, files) in os.walk("/home/ubuntu/dump_ingest/GCIS_2015-07-28_v0/dataset"):
#	for f in files:
#		localList.append#id

#pull json items into list
 
