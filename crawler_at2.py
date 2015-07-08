__author__ = 'torresal'

import os,re, subprocess
#/home/ubuntu/dump_ingest/2015-07-02_v0/reports
stpath = "/home/ubuntu/dump_ingest/2015-07-02_v0"
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
            i ="{}_ingest.py {}_{}.json" .format(t, t, id)
            subprocess.call('python /home/ubuntu/dump_ingest/ingest_scripts/{}/{}' .format(t,i), shell=True)
            #print('python /home/ubuntu/dump_ingest/ingest_scripts/report/{}' .format(i))




