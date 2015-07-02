__author__ = 'torresal'
# Personal notes
    # path = “{starting_path}/{type}/{identifier}.json”
    # ingest.py {starting path} is Dexters script
    # Loop statement : for iterating_var in sequence:

import os,re, subprocess

stpath = "/Users/torresal/2015-06-22_v0"
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
            i ="ingest_{}.py {}_{}.json" .format(t, t, id)
            subprocess.call('python /Users/torresal/{}' .format(i), shell=True)
            #print('python /Users/torresal/{}' .format(i))





