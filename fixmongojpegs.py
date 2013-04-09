#!/usr/bin/python2.7

# This should fix some problems
# Other systems HAVE TO BE OFF or there will be concurrency problems

import os
import time
from pymongo import MongoClient, ASCENDING
import MySQLdb as mysql




client = MongoClient()
jpegsCollection = client.crawl.jpegs

# mongodb data example
"""
{
	"_id" : ObjectId(someHexNumberString),
	"domain" : "example.com",
	"tries" : 1,  #number of tries to download 
	"location" : "example.com/some.jpg", ### Where we found it on the internet
	"local_location" : "pics/example.com%some.jpg", ### Note the % replacement so we can save it
	"saved" : true ## Whether we saved it or not
    "imagesId": 105 # should be in crawl.images mysql database
    "lock" : 4756 # pid of program saving it may have to clean these out if suddenly fails
}
"""





# GET RID OF BAD IDEA

#jpegs = jpegsCollection.find()

#for peg in jpegs:
#    if "local_location" in peg.keys() and peg['local_location']:
#        pos = peg['local_location'].find("pics/")
#        if pos == 0:
#            peg['local_location'] = peg['local_location'][5:]


conn = mysql.connect(host="localhost", user="pythonscript", passwd="182182", db="crawl")
cursor = conn.cursor()

jpegs = jpegsCollection.find({"saved": True})

for peg in jpegs:
    # here is the terrible thing we have to go through to get this thing from the db which is why we are saving the id here
    if type( peg['local_location'] ) == type(u"a string") or type( peg['local_location'] ) == type("a string"):
        cursor.execute('SELECT Id FROM images WHERE filename = "%s"' % peg['local_location'] )
    else:
        print peg, " has no local location"
    Id = cursor.fetchone()
    if Id:    
        Id = Id[0]
    else:
        print peg, " not found in db"
    peg['imagesId'] = Id
    jpegsCollection.save(peg)


















