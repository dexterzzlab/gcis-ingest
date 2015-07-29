__author__ = 'Dexter Tan'

import os,re, subprocess, sys
from datetime import datetime
startTime = datetime.now()

#Pass in location of JSON files by command line argument
dump_directory = sys.argv[1]

#ingest_directory = "/home/ubuntu/dump_ingest/ingest_scripts"
ingest_file = "/home/ubuntu/dump_ingest/prov_es_ingest.py"


#Correct dump_directory
if not dump_directory.endswith("/"):
    dump_directory+"/"

#runs python call: python [TYPE]_ingest.py [DIR]/[NAME].json
for (root, dirs, files) in os.walk(dump_directory):
    for f in files:
        fileType = f.split("_")[0]
        fullFilePath = os.path.realpath(os.path.join(root,f))
        shellCall = "python {} {}".format(ingest_file, fullFilePath)
        subprocess.check_call(shellCall, shell=True)


time_elapsed =  (datetime.now() - startTime)

f = open('prov_es_crawl_log.txt', 'w')

f.write("{} crawler finished. time elapsed: {}\n".format(datetime.now(), time_elapsed))
f.close()
