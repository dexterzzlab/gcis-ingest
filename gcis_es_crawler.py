__author__ = 'Dexter Tan'

import os,re, subprocess, sys
import datetime
#from datetime import datetime
startTime = datetime.datetime.now()

logFile = str(datetime.date.today())+"_gcis_ingest_log.txt"

log = open(logFile, 'w')

#Pass in location of JSON files by command line argument
if len(sys.argv) > 1:
    dump_directory = sys.argv[1]
else:
    dump_directory = "GCIS_%s_v0" % datetime.date.today()
    print dump_directory
    #dump_directory = sys.argv[2]
#ingest_directory = "/home/ubuntu/dump_ingest/ingest_scripts"
ingest_file = "/home/ubuntu/dump_ingest/gcis_es_ingest.py"


#Correct dump_directory
if not dump_directory.endswith("/"):
    dump_directory+"/"

#runs python call: python [TYPE]_ingest.py [DIR]/[NAME].json
for (root, dirs, files) in os.walk(dump_directory):
    for f in files:
        fileStart = datetime.datetime.now()
        fileType = f.split("_")[0]
        fullFilePath = os.path.realpath(os.path.join(root,f))
        shellCall = "python {} {} {}".format(ingest_file, fileType, fullFilePath)
        subprocess.check_call(shellCall, shell=True)
        log.write("{} to complete: {}\n".format((datetime.datetime.now() - fileStart), f))

time_elapsed =  (datetime.datetime.now() - startTime)

log.write("{} crawler finished. time elapsed: {}\n".format(str(datetime.datetime.now()), time_elapsed))
log.close()
