__author__ = 'torresal'

import os,re, subprocess
#/home/ubuntu/dump_ingest/2015-07-09_v0
stpath = "/home/ubuntu/dump_ingest/2015-07-09_v0"
#insert path to file#

for (root,dirs,files) in os.walk(stpath):
      files.sort()
      for file in files:
        dir_path = os.path.join(root, file)
        #extracts the root, dirs, files from iven path#
        #print(dir_path)
        match =  re.search(r'.*/(.*?)_(.*?)\.json$', dir_path)
        if match:
            t, id = match.groups()
            #print ('match',t, id)
            i ="/home/ubuntu/dump_ingest/ingest_scripts/{}_ingest.py" .format(t)
            p = "{}/{}/{}_{}.json" .format(stpath,t,t, id)
            subprocess.call('python {} {}' .format(i,p), shell=True)
            #print('python {} {}' .format(i,p))




