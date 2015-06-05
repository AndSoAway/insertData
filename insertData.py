import pymongo
import time
import dtReader

begin = time.clock()
client = pymongo.MongoClient('166.111.71.115', 30000)
db = client.rawdata
filePath = 'HD_20150311_000000_SCADA.DT'
dtReader = dtReader.DtReader(filePath)
#dtReader.insertData(db)
end = time.clock()

print(end - begin)