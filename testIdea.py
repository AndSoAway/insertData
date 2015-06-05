str = '<sysystem>  '
def testSplit():
	file = open('testData.DT', 'r')
	#str = file.readline()[:-1]
	#str = file.readline()[:-1]
	#print(str)
	#print(str.split(' '))
	i = 0
	for line in file:
		i = i + 1
		print(line)
		for newLine in file:
			print(newLine)
			break
			if i == 4:
				break
	file.close()

def testSplitEmpty():
	file = open('testData.DT', 'r')
	str1 = file.readline()
	str2 = file.readline()
	print(str1)
	i = 0
	for str2 in file:
		subWordList = str2[:-1].split(' ')
		subWordNotEmpty = []
		#print(subWordList)
		for word in subWordList:
			if len(word) > 0:
				subWordNotEmpty.append(word)
		print(subWordNotEmpty)
		i = i + 1
		if i == 10:
			break


if  __name__ == '__main__':
	testSplitEmpty()