__author__ = 'torresal'
# Personal notes
    # path = “{starting_path}/{type}/{identifier}.json”
    # ingest.py {starting path} is Dexters script
    # Loop statement : for iterating_var in sequence:
import os,re

stpath = "/Users/torresal/2015-06-22_v0/articles"

for (root,dirs,files) in os.walk(stpath):
      files.sort()
      for file in files:
        dir_path = os.path.join(root, file)
        #print(dir_path)
        match =  re.search(r'.*/(.*?)_(.*?)\.json$', dir_path)
        if match:
            t, id = match.groups()
            #print ('match',t, id)
            cmd ="echo 'ingest_{}.py {}_{}.json'" .format(t, t, id)
            os.system(cmd)





