import os

logFile = open('test.txt', 'r')
fullNameList = []
for line in logFile:
    fullName =  line.split(': ')[-1]
    if fullName.split('_')[0] == 'dataset':
        fullNameList.append(fullName[:-1])
        #print fullName
        #if not os.path.isfile(""+fullName)
        #    print fullName
#print fullNameList

#print len(fullNameList) == len(set(fullNameList))
setList = []
count = 0
for (root, dirs, files) in os.walk("/home/ubuntu/dump_ingest/GCIS_2015-07-28_v0/dataset"):
    #if files not in fullNameList:
        #print files
    for f in files:
        setList.append(f)
        #if f not in fullNameList:
        #    print f
            #            print "/"
#        else:
#            print str(count) +  f
#        count +=1

print len(setList)
print len(setList) == len(set(setList))
