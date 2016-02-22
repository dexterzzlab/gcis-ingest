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
            "/image/",
            "/activity/",
            "/lexicon/",
            "/model/",
            "/gcmd_keyword/",
            "/region/",
            "/project/",
            "/generic/",
            "/webpage/",
            "/reference/",
            "/array/",
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
            'http://data.globalchange.gov/activity.json?all=1',
            'http://data.globalchange.gov/lexicon.json?all=1',
            'http://data.globalchange.gov/model.json?all=1',
            'http://data.globalchange.gov/gcmd_keyword.json?all=1',
            'http://data.globalchange.gov/region.json?all=1',
            'http://data.globalchange.gov/project.json?all=1',
            'http://data.globalchange.gov/generic.json?all=1',
            'http://data.globalchange.gov/webpage.json?all=1',
            'http://data.globalchange.gov/reference.json?all=1',
            'http://data.globalchange.gov/array.json?all=1',

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
    
    fileID = 0
    for block in allJson:
        #name =  href field 
        fileName = block['href']
        #remove .json
        fileName = fileName[:-5]
        #remove url
        fileName = fileName.replace("http://data.globalchange.gov/", "")
        #remove special characters - necessary for articles
        #fileName = re.sub(r'\W+', '_', fileName)
        fileName = fileName.split("/")

        #fileNameNoJSON = ""#"\\?\ "

        fileNameNoJSON = fileName[0]

        fileName = fileName[1:]

        for part in fileName:
            fileNameNoJSON = "%s_%s"%(fileNameNoJSON, part)
        

        #fileName = "%s_%s"%(fileName[0],fileName[1])
        
        #fileNameNoJSON = fileName

        fileName = fileNameNoJSON
        if len(fileName) > 120:
            fileName = fileName[:120]
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
    
        #index chapters and findings
        elif fileDir[x] is "/report/":
            with open(str(fileName), 'w') as jsonFile:
                jsonFile.write(json.dumps(block, sort_keys=True, indent=4, separators=(',',': ')))
            

            #chapter and finding
            chapterAll = requests.get("http://data.globalchange.gov/report/%s/chapter.json"%(block['identifier']))#requests.get("%s%s/%s/chapter.json?all=1".())
            findingAll = requests.get("http://data.globalchange.gov/report/%s/finding.json"%(block['identifier']))
            
            #if chapter works...
            if chapterAll.status_code == 200:
                chapterAll.raise_for_status()
                chapters = chapterAll.json()
                
                noneCounter = 0
                chapterSubDir = "%s/chapter"%topDirectory
                chapterReportSubDir = "%s/%s"%(chapterSubDir, fileNameNoJSON)
                existingChapters = []
                for chapter in chapters:
                    chapterName = "%s_chapter_%s"%(fileNameNoJSON, chapter['identifier'])
                    #if chapter['number'] not in existingChapters:
                    if chapterName not in existingChapters:
                        existingChapters.append(chapter['number'])
                        #chapterName = "%s-chapter-%s.json"%(fileNameNoJSON, chapter['number'])
                    else:
                        print "Duplicate %s"%chapterName

                                    #if not os.path.isdir("%s/chapter"%topDirectory):
                    if not os.path.exists(chapterReportSubDir):
                        os.makedirs(chapterReportSubDir)
                    with open(str("%s/chapter/%s/%s.json"%(topDirectory, fileNameNoJSON, chapterName)), 'w') as chapterFile:
                        chapterFile.write(json.dumps(chapter, sort_keys=True, indent=4, separators=(',', ': ')))

            findingList = []
            if findingAll.status_code == 200:
                findingAll.raise_for_status()
                findings = findingAll.json()

                findingDir = "%s/finding"%topDirectory
                #findingReportSubDir = "%s/%s"%(findingDir,fileNameNoJSON)
                #if not os.path.exists(findingReportSubDir):
                #    os.makedirs(findingReportSubDir)
                if not os.path.exists(findingDir):
                    os.makedirs(findingDir)
                for finding in findings:
                    
                    
                    #print finding['identifier']
                    if finding['identifier'] in findingList:
                        print "Found duplicate %s" %finding['identifier']
                    findingList.append(finding['identifier'])
                                    #findingAll = requests.get()
                    with open(str("%s/%s.json"%(findingDir,finding['identifier'])), 'w') as findingFile:
                        findingFile.write(json.dumps(finding, sort_keys=True, indent=4, separators=(',', ': ')))



        else:
            with open(str(fileName), 'w') as jsonFile:
                jsonFile.write(json.dumps(block, sort_keys=True, indent=4, separators=(',', ': ')))
        fileID += 1





