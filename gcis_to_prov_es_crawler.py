import os, sys, datetime, subprocess

#from datetime import datetime
startTime = datetime.datetime.now()
gcisDir = sys.argv[1]
prov_es_json_file = "/home/ubuntu/dump_ingest/gcis_to_prov_es_json.py"
topDirectory = "PROV_ES_" + str(datetime.date.today()) + "_v"
dirCount = 0
#print topDirectory
while os.path.isdir(topDirectory + str(dirCount)):
    dirCount += 1

topDirectory = topDirectory + str(dirCount)


# python call: python [TYPE]_ingest.py [DIR]/[NAME].json
for (root, dirs, files) in os.walk(gcisDir):
    for f in files:
        fileStart = datetime.datetime.now()
        fullFilePath = os.path.realpath(os.path.join(root,f))
        shellCall = "python {} {} {}".format(prov_es_json_file, fullFilePath, topDirectory)
        subprocess.check_call(shellCall, shell=True)
        fileEnd = datetime.datetime.now()

time_elapsed = (datetime.now() - startTime)

log = open("prov_es_dump_log.txt", 'w')

log.write("{} prov_es_dump finished. time elapsed: {}\n".format(datetime.now(), time_elapsed))
log.close()
