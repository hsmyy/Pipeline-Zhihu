__author__ = 'fc'

MONGO_HOST='rey'
MONGO_PORT='27017'
MONGO_DB='zhihu'
MONGO_COLLECTION=''
MONGO_JOIN_COLLECTION=''

TEMP_FILE='temp.txt'

import pymongo

def join():
    client = pymongo.MongoClient(MONGO_HOST,MONGO_PORT)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    join = db[MONGO_JOIN_COLLECTION]
    # 1st step: write to temp file
    with open(TEMP_FILE,'w') as f:



        for item in collection.find():
            f.write(item['url'] + '\r\n')

    # 2nd find ans, merge and update
    with open(TEMP_FILE,'r') as f:
        for line in f.readlines():
            line = line.strip('\r\n')
            anss = []
            for ans in join.find({'url': line}):
                anss.append(ans)
            ansContent = '\t'.join(anss)
            collection.update({'url':line},{'$set':{'ansContent': ansContent}})

    client.close()