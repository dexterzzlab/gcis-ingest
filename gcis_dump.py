import requests
import json
import os
import datetime
import re

fileDir = [
            "/figure/",
            "/person/",
            "/report/",
            "/journal/",
            "/scenario/",
            "/book/",
            "/model/",
            "/instrument/",
            "/article/",
            "/platform/",
            "/organization/",
            "/dataset/",
            "/image/"
            ]

reqList = [
           'http://data.globalchange.gov/figure.json?all=1',
           'http://data.globalchange.gov/person.json?all=1',
           'http://data.globalchange.gov/report.json?all=1',
           'http://data.globalchange.gov/journal.json?all=1',
           'http://data.globalchange.gov/scenario.json?all=1',
           'http://data.globalchange.gov/book.json?all=1',
           'http://data.globalchange.gov/model.json?all=1',
           'http://data.globalchange.gov/instrument.json?all=1',
           'http://data.globalchange.gov/article.json?all=1',
           'http://data.globalchange.gov/platform.json?all=1',
           'http://data.globalchange.gov/organization.json?all=1',
           'http://data.globalchange.gov/dataset.json?all=1',
           'http://data.globalchange.gov/image.json?all=1',
           ]


#setup directories for file storage

dirCount = 0
topDirectory = "GCIS_"+str(datetime.date.today()) + "_v"

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
    
    fileID = 0
    for block in allJson:
        #name =  href field 
        fileName = block['href']
        #remove .json
        fileName = fileName[:-5]
        #remove url
        fileName = fileName.replace("http://data.globalchange.gov/", "")
        #remove special characters - necessary for articles
        fileName = re.sub(r'\W+', '_', fileName)
        #reappend .json tag
        fileName = fileName + ".json"

        #exception for figure
        if fileDir[x] is "/figure/":
            fileName = fileName.split("_", 1)[-1]
            fileName = "figure_" + fileName
        #    print fileName
        #re-build full file path
        fileName = fileDirectory + fileName

        if fileDir[x] is "/figure/":
            figBlock = requests.get(block['href']).json()
            with open(str(fileName), 'w') as jsonFile:
                jsonFile.write(json.dumps(figBlock, sort_keys=True, indent=4, separators=(',', ': ')))
        elif fileDir[x] is "/image/": #if x is image
            imgBlock = requests.get(block['href']).json()
            #print "hit exception" 
            with open(str(fileName), 'w') as jsonFile:
                jsonFile.write(json.dumps(imgBlock, sort_keys=True, indent=4, separators=(',', ': '))) 
        else:
            with open(str(fileName), 'w') as jsonFile:
                jsonFile.write(json.dumps(block, sort_keys=True, indent=4, separators=(',', ': ')))
        fileID += 1





