__author__ = 'Dexter Tan, torresal'

import os,subprocess, sys

#Pass in location of JSON files by command line argument
dump_directory = sys.argv[1]

#Master ingest script
ingest_file = "/home/ubuntu/dump_ingest/general_ingest.py"

#Correct dump_directory
if not dump_directory.endswith("/"):
    dump_directory+"/"

for (root,dirs,files) in os.walk(dump_directory):
      files.sort()
      for file in files:
        fileType = file.split("_")[0]
        filePath = os.path.join(root, file)  
        v = 'python {} {} {}' .format(ingest_file, fileType, filePath)
        subprocess.call(v, shell=True)
        


