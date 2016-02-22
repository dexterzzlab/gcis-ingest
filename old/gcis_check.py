import requests
import json
import os
import datetime
import re

fileDir = [
#            "/figure/",
#            "/person/",
#            "/report/",
#            "/journal/",
#            "/scenario/",
#            "/book/",
#            "/model/",
#            "/instrument/",
            "/article/",
#            "/platform/",
#            "/organization/",
#            "/dataset/",
#            "/image/",
#            "/activity/",
#            "/lexicon/",
#            "/model/",
#            "/gcmd_keyword/",
#            "/region/",
#            "/project/",
#            "/generic/",
#            "/webpage/",
#            "/reference/",
#            "/array/",
            ]

reqList = [
 #           'http://data.globalchange.gov/figure.json?all=1',
 #           'http://data.globalchange.gov/person.json?all=1',
 #           'http://data.globalchange.gov/report.json?all=1',
 #           'http://data.globalchange.gov/journal.json?all=1',
 #           'http://data.globalchange.gov/scenario.json?all=1',
 #           'http://data.globalchange.gov/book.json?all=1',
 #           'http://data.globalchange.gov/model.json?all=1',
 #           'http://data.globalchange.gov/instrument.json?all=1',
            'http://data.globalchange.gov/article.json?all=1',
 #           'http://data.globalchange.gov/platform.json?all=1',
 #           'http://data.globalchange.gov/organization.json?all=1',
 #           'http://data.globalchange.gov/dataset.json?all=1',
 #           'http://data.globalchange.gov/image.json?all=1',
 #           'http://data.globalchange.gov/activity.json?all=1',
 #           'http://data.globalchange.gov/lexicon.json?all=1',
 #           'http://data.globalchange.gov/model.json?all=1',
 #           'http://data.globalchange.gov/gcmd_keyword.json?all=1',
 #           'http://data.globalchange.gov/region.json?all=1',
 #           'http://data.globalchange.gov/project.json?all=1',
 #           'http://data.globalchange.gov/generic.json?all=1',
 #           'http://data.globalchange.gov/webpage.json?all=1',
 #           'http://data.globalchange.gov/reference.json?all=1',
 #           'http://data.globalchange.gov/array.json?all=1',

           ]


#setup directories for file storage

dirCount = 0
topDirectory = "GCIS_DUMP/GCIS_"+str(datetime.date.today()) + "_v"

while os.path.isdir(topDirectory + str(dirCount)):
    dirCount += 1

topDirectory = topDirectory + str(dirCount)

os.makedirs(topDirectory)

for x in range(len(fileDir)):


    fileDirectory = topDirectory + fileDir[x]

    if not os.path.isdir(fileDirectory):
        os.makedirs(fileDirectory)

    req = requests.get(reqList[x])#'http://data.globalchange.gov/book.json?all=1')

    allJson = req.json()
    count = 0   
    fileID = 0
    foundID = []
    for block in allJson:
        count += 1
        #name =  href field 
        fileName = block['href']
        #remove .json
        fileName = fileName[:-5]
        #remove url
        fileName = fileName.replace("http://data.globalchange.gov/", "")
        #remove special characters - necessary for articles
        #fileName = re.sub(r'\W+', '_', fileName)
        fileName = fileName.split("/")

        #fileName = "%s_%s"%(fileName[0],fileName[1])
        fileNameNoJSON = ""
        for x in fileName:
            fileNameNoJSON = "%s_%s"%(fileNameNoJSON, x)
        #fileNameNoJSON = fileName

        if fileNameNoJSON not in foundID:
            foundID.append(fileNameNoJSON)
        else:
            print fileName

        #reappend .json tag
        fileName = fileNameNoJSON + ".json"
        
        fileID += 1
        with open(str(fileName), 'w') as jsonFile:
            jsonFile.write(json.dumps(block, sort_keys=True, indent=4, separators=(',', ': ')))
        
        #print count

