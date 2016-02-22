__author__ = 'Dexter Tan'

import os,re, subprocess, sys
import datetime
#from datetime import datetime
startTime = datetime.datetime.now()

logFile = str(datetime.date.today())+"_gcis_ingest_log.txt"

log = open(logFile, 'w')

#Pass in location of JSON files by command line argument
if len(sys.argv) > 2:
    if sys.argv[1].endswith(".py"): 
        ingest_file = sys.argv[1]#"gcis_es_ingest.py"


        dump_directory = sys.argv[2]
   #print dump_directory
    #dump_directory = sys.argv[2]
#ingest_directory = "/home/ubuntu/dump_ingest/ingest_scripts"
    
#Correct dump_directory
        if not dump_directory.endswith("/"):
            dump_directory+"/"

#print dump_directory

#runs python call: python [TYPE]_ingest.py [DIR]/[NAME].json
        for (root, dirs, files) in os.walk(dump_directory):
            for f in files:
                #print f
                fileStart = datetime.datetime.now()
                #fileType = f.split("_")[0]
                
                fullFilePath = os.path.realpath(os.path.join(root,f))
                
                fileType = fullFilePath.split("/")[-2]
                
                shellCall = "python {} {} {}".format(ingest_file, fileType, fullFilePath)

                

                print shellCall
                subprocess.check_call(shellCall, shell=True)
                log.write("{} to complete: {}\n".format((datetime.datetime.now() - fileStart), f))

        time_elapsed =  (datetime.datetime.now() - startTime)

        log.write("{} crawler finished. time elapsed: {}\n".format(str(datetime.datetime.now()), time_elapsed))
        log.close()
    else:
        print "sys.argv[1] not a correct ingest script"

else:
    print "sys.argv[1] must be ingest file, sys.argv[2] must be dump directory"
    #dump_directory = "GCIS_DUMP/GCIS_%s_v0" % datetime.date.today()
 
