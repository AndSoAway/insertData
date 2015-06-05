#!/usr/bin/env python
#-*- utf-8 -*-

import pymongo
import time

class DtReader:
	'''解析DT文件，XML格式'''
	insertBatchNum = 10000
	begin = 0
	end = 0
	insertTime = 0
	insertCount = 0
	def __init__(self):
		self.logFile = open('log.txt', 'w')
		self.copyFile = open('copy.txt', 'w')

	def __del__(self):
		self.logFile.close()

	def insertData(self, db, filePath):
		self.insertTime = 0
		self.insertCount = 0
		self.file = open(filePath, mode='r', encoding='GB18030', errors='ignore')
		self.db = db
		for line in self.file:
			#self.copyFile.write('read: ' + line)
			if line[:2] == '</':
				#print("begin with </ " + line)
				continue
			if line[0] == '<':
				#print("begin with <  " + line)
				self.collectionName = line[1:line.find('>')]
				#print(self.collectionName)
				#self.logFile.write(self.collectionName)
				
				#如果是第一个标签system
				if(self.collectionName == 'system'):
					i = 0
					for sonLine in self.file:
						#self.copyFile.write('read: ' + sonLine)
						#print(sonLine)
						i = i + 1
						#抽取时间
						if i == 2:
							subWordList = sonLine.split(' ')
							wordIndex = 0
							for subWord in subWordList:
								if len(subWord) > 0:
									wordIndex= wordIndex + 1
								if wordIndex == 4:
									self.timestamp = subWord[:-1]
									#self.logFile.write(self.timestamp)
									#print(self.timestamp)
							break
				continue
					
			#如果不是第一个标签,则开始插入新的collection	
			self.collection = db[self.collectionName]
			#获取collection的key
			self.keyList = self.extractLine(line)
			self.keyList.append('timestamp')
			#print(self.keyList)
				
			num = 0
			insertKVList = []
			for sonLine in self.file:
				#self.copyFile.write('read: ' + sonLine)
				if sonLine[:2] == '</':
					break
				self.valueList = self.extractLine(sonLine)
				self.valueList.append(self.timestamp)
				num = num + 1
				if num <= self.insertBatchNum:
					self.insertCount += 1
					insertKVList.append(dict(zip(self.keyList, self.valueList)))
					continue
				#self.logFile.write(str(insertKVList))
				self.begin = time.clock()
				self.collection.insert_many(insertKVList)
				self.end = time.clock()
				self.insertTime += self.end - self.begin
				num = 0
				insertKVList = []
				#print("insert 1000")
			self.begin = time.clock()
			self.collection.insert_many(insertKVList)
			self.end = time.clock()
			self.insertTime += self.end - self.begin
		self.file.close()

	def extractLine(self, line):
		subWordList = line[:-1].split(' ')
		wordIndex = 0
		wordList = []
		for subWord in subWordList:
			if len(subWord) > 0:
				wordIndex += 1
				if wordIndex > 1:
					wordList.append(subWord)
		return wordList


if __name__ == '__main__':
	begin = time.clock()
	client = pymongo.MongoClient('166.111.71.115', 30000)
	db = client.rawdata
	filePath = 'HD_20150311_001500_SCADA.DT'
	dtReader = DtReader(filePath)
	dtReader.insertData(db)
	end = time.clock()

	print('run time:' + str(end - begin))
	print('insert time:' + str(dtReader.insertTime))
	print('insert count:' + str(dtReader.totalcount))		
