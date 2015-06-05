#!/usr/bin/env python
#-* coding=utf-8-*-

import pymongo
import time
import csv
import os
import hzinserter

begin = time.clock()

client = pymongo.MongoClient('166.111.71.115', 30000)
db = client.rawdata
hzInserter = hzinserter.HzInserter()

countOfAllData = 0
timeOfAllData = 0

hzLogCsvFile = open('hzLogFile.csv', 'w+')
writer = csv.writer(hzLogCsvFile)
writer.writerow(['fileName', 'insertCount', 'insertTime', 'insertVel'])

rawHzDataDir = 'rawData/rawData/hz_file/'
hzFileNameList = os.listdir(rawHzDataDir)
for hzFileName in hzFileNameList:
	hzFilePath = rawHzDataDir + hzFileName
	print('begin insert: ' + hzFileName)
	hzInserter.insertData(db, hzFilePath)
	print('insertTime: ' + str(hzInserter.insertTime) + ' insertCount: ' + str(hzInserter.insertCount))
	countOfAllData += hzInserter.insertCount
	timeOfAllData += hzInserter.insertTime
	writer.writerow([hzFileName, str(hzInserter.insertCount), str(hzInserter.insertTime), str(hzInserter.insertCount / hzInserter.insertTime)])

writer.writerow(['sumOfAllFile', str(countOfAllData), str(timeOfAllData), str(countOfAllData / timeOfAllData)])
hzLogCsvFile.close()
end = time.clock()
print('run time:' + str(end - begin))