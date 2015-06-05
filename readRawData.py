#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import gzip
import dtReader
import csv
import time
import pymongo

begin = time.clock()

client = pymongo.MongoClient('166.111.71.115', 30000)
db = client.rawdata
inserter = dtReader.DtReader()

countOfData = 0
timeOfData = 0

velFile = open('insertVel.csv', 'w+')
writer = csv.writer(velFile)
writer.writerow(['fileName', 'insertCount', 'insertTime', 'insertVel'])

rawDataDir = 'rawData/rawData/scada_file/'
dataFileNameList = os.listdir(rawDataDir)
for dataFileName in dataFileNameList:
	if dataFileName[-2:] != 'DT':
		continue
	dataFilePath = rawDataDir + dataFileName
	print('insertFile:' + dataFileName)
	inserter.insertData(db, dataFilePath)
	print('insert time:' + str(inserter.insertTime))
	timeOfData += inserter.insertTime
	print('insert count:' + str(inserter.insertCount))
	countOfData += inserter.insertCount
	writer.writerow([dataFileName, str(inserter.insertCount), str(inserter.insertTime), str(inserter.insertCount / inserter.insertTime)])

writer.writerow(['sumOfAllFile', str(countOfData), str(timeOfData), str(countOfData / timeOfData)])
velFile.close()
end = time.clock()
print('run time:' + str(end - begin))