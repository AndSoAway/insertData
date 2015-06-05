#!/usr/bin/env python
#-*- coding=utf-8 -*-

import pymongo
import time
import dt
begin = time.clock()
client = pymongo.MongoClient('166.111.71.115', 30000)
db = client.localInsertTest

end = time.clock()