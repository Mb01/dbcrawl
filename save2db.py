#!/usr/bin/python2.7

# This saves all the newline delimited urls in a file to files in a pic directory
# you need to make this directory first cause lazy am i.

import argparse
import urllib
from urllib2 import urlopen, URLError, HTTPError
from urlparse import urlparse
import os
import httplib
import time
import sys
import inspect

PID = os.getpid()

from pymongo import MongoClient, ASCENDING

client = MongoClient()
jpegsCollection = client.crawl.jpegs

import MySQLdb as mysql

conn = mysql.connect(host="localhost", user="root", passwd="182182", db="crawl")

cursor = conn.cursor()
# mongodb data example
"""
{
	"_id" : ObjectId(someHexNumberString),
	"domain" : "example.com",
	"tries" : 1,  #number of tries to download 
	"location" : "example.com/some.jpg", ### Where we found it on the internet
	"local_location" : "pics/example.com%some.jpg", ### Note the % replacement so we can save it
	"saved" : true ## Whether we saved it or not
    "lock" : 4756 # pid of program saving it may have to clean these out if suddenly fails
}
"""

def savePic( url ):# Database version
    #ensure we haven't already saved the image
    url = url.encode('utf8')
    p = urlparse( url )
    filestring = str( p.netloc + p.path ).replace("/","%") #for now 
    query = "SELECT Id FROM Images WHERE filename = %s"
    cursor.execute( query, filestring )
    if cursor.fetchone():
        return filestring
    # Download and insert into database
    print "trying %s" % url
    data = urlopen( url ).read()
    if len(data) < 200:
        return None
    query = "INSERT INTO Images (data, time, filename) VALUES ('%s','%s','%s')" % \
                ( mysql.escape_string(data),
                  str(time.time()), 
                  mysql.escape_string(filestring) )
    cursor.execute( query )
    conn.commit()
    return filestring

while True:
    # INDEXING

    jpegsCollection.ensure_index([("saved", ASCENDING)] , background=True, cache_for=600)
    jpegsCollection.ensure_index([("lock" , ASCENDING)] , background=True, cache_for=600)

    # QUERY FOR TARGETS

    toReserve = jpegsCollection.find( {"saved" : False, "tries" : 0, 'lock': None }, limit=300 )
    if toReserve.count() == 0:
        toReserve = jpegsCollection.find( {"saved" : False, "tries" :{ '$lt':2 }, 'lock': None }, limit=50 )

    # RESERVE TARGETS

    for jpeg in toReserve:
        jpeg['lock'] = PID
        jpegsCollection.save(jpeg)
    time.sleep(1) # thrown in for good measure...

    # GET OUR RESERVATIONS

    toLoad = jpegsCollection.find( {"saved" : False, "tries" : 0, 'lock': PID }, limit=600 ) # shouldn't be above 300
    if toLoad.count() == 0:
        time.sleep(5) # when we are running low we should wait more
        toLoad = jpegsCollection.find( {"saved" : False, "tries" :{ '$lt':2 }, 'lock': PID }, limit=50 )
    for jpeg in toLoad:
        try:
            location = savePic( jpeg['location'] )
            jpeg['local_location'] = location
            jpeg['saved'] = True
            jpeg['lock'] = None
            jpeg['tries'] += 1
            jpegsCollection.save(jpeg)
        except (IOError, URLError, UnicodeError, HTTPError, ValueError, AttributeError,httplib.IncompleteRead) as e:
            jpeg['tries'] += 1
            jpeg['lock'] = None
            if jpeg['saved'] == True:
                raise Exception # because we should not be getting here
            jpegsCollection.save(jpeg)
            print e
        time.sleep(1)
    time.sleep(10) # so that when we run out of pictures we don't hammer the database

