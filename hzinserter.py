#!/usr/bin/env python
#-*-utf-8-*-

import time
class HzInserter:
	'''inserter hz file'''
	keyList = ['timestamp', 'hz']
	insertTime = 0
	insertCount = 0

	def insertData(self, db, filePath):
		self.insertTime = 0
		self.insertCount = 0
		self.file = open(filePath, 'r')
		print(self.file.readline())
		collection = db['hz']
		kvList = []
		for line in self.file:
			self.insertCount += 1
			subWordList = line[:-1].split(' ')
			date = subWordList[0]
			secondWordList = subWordList[1].split('\t')
			timestamp = date + 'T' + secondWordList[0]
			hzData = secondWordList[1]

			valueList = [timestamp, hzData]
			kvList.append(dict(zip(self.keyList, valueList)))
		begin = time.clock()
		collection.insert_many(kvList)
		end = time.clock()
		self.insertTime = end - begin
		self.file.close()

if __name__ == '__main__':
	db = ''
	filePath = 'rawData/rawData/hz_file/FREQUENCE_201503110000.DT'
	hzInserter = HzInserter()
	hzInserter.insertData(db, filePath)