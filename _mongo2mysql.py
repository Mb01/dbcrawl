#!/usr/bin/python2.7

# This is going to move all the data into a better db and reformat everything

import os
import time
from pymongo import MongoClient, ASCENDING
import MySQLdb as mysql
import sys
# mongodb data example
# crawl.jpegs
"""
{
	"_id" : ObjectId(someHexNumberString),
	"domain" : "example.com",
	"tries" : 1,  #number of tries to download 
	"location" : "example.com/some.jpg", ### Where we found it on the internet
	"local_location" : "pics/example.com%some.jpg", ### Note the % replacement so we can save it to filesystem
	"saved" : true ## Whether we saved it or not
}
"""

# mongodb data example
# crawl.links
"""
{
	"_id" : ObjectId("5157fa18ea82411dfc93f8f3"),
	"domain" : "garss.tv",
	"checked" : true,
	"links" : [ ],
	"read" : true,
	"location" : "http://garss.tv/adult",
	"visited" : true,
	"contents" : null
}
"""

# MySQL example
# testdb Images
"""
+----------+--------------+------+-----+---------+----------------+
| Field    | Type         | Null | Key | Default | Extra          |
+----------+--------------+------+-----+---------+----------------+
| Id       | int(11)      | NO   | PRI | NULL    | auto_increment |
| data     | mediumblob   | YES  |     | NULL    |                |
| time     | varchar(30)  | YES  |     | NULL    |                |
| filename | varchar(200) | YES  |     | NULL    |                |
+----------+--------------+------+-----+---------+----------------+
"""


#
#       GETTING OUR MYSQL CURSOR AND CONNECTION FOR testdb
#                                                   ^^^^^^

HOST = "localhost"
USER = 'testuser'
PASSWORD = 'test623'
testConnection = mysql.connect(host=HOST,user=USER,passwd=PASSWORD, db='testdb')

testCursor = testConnection.cursor()

#
#   GETTING OUR MONGODB CURSOR AND CONNECTION FOR jpegs
#
client = MongoClient()

jpegsCollection = client.crawl.jpegs
linksCollection = client.crawl.links


#   FIRST WE NEED THE CURSOR AND SOME NEW TABLES IN THE NEW DB "crawl"


make_Jpegs_Table_Query = """
    CREATE  TABLE IF NOT EXISTS jpegs
     (IDjpegs INT PRIMARY KEY AUTO_INCREMENT,
     domain VARCHAR(128),
     tries TINYINT,
     data MEDIUMBLOB,
     location VARCHAR(256),
     saved  TINYINT(1))
"""

make_Links_Table_Query = """
    CREATE  TABLE IF NOT EXISTS links
  (IDlinks INT PRIMARY KEY AUTO_INCREMENT,
domain VARCHAR(128),
checked TINYINT(1),
gotten TINYINT(1),
visited TINYINT(1),
location VARCHAR(255),
contents TEXT)
CHARSET=utf8
"""

PYHOST = "localhost"
PYUSER = 'pythonscript'
PYPASSWORD = '182182'
crawlConnection = mysql.connect(host=PYHOST,user=PYUSER,passwd=PYPASSWORD, db='crawl', charset='utf8')

# WE USE THIS
crawlCursor = crawlConnection.cursor()

# NOTE HOW WE HAVE CURSORS FOR MYSQL AND COLLECTIONS FOR MONGODB

# WE ARE GOING TO MOVE INFORMATION FROM THESE

# testcursor -> MYSQL CURSOR
# jpegsCollection -> MONGODB COLLECTION
# linksCOllection -> MONGODB COLLECTION

# TO THIS

# crawlCursor


########################## DO WE HAVE THE TABLES?
########################## UNCOMMENT / COMMENT FOR DESIRED EFFECT
crawlCursor.execute( make_Jpegs_Table_Query )
crawlCursor.execute( make_Links_Table_Query )



########################## THIS IS TOO SLOW


# POPULATE THE JPEGS TABLE

#mongoJpegs = jpegsCollection.find(timeout=False)
#
#for jpeg in mongoJpegs:
#
#    try:
#        #get the binary data in the mysql database and the date
#        query = "SELECT data, time FROM Images WHERE filename = %s"
#        if jpeg['saved']:
#            testCursor.execute( query, jpeg['local_location'][5:] )
#            data, creation_time = testCursor.fetchone()
#        else:
#            data = ""
#            creating_time = "0"
#    ### NOT FINISHED EDITING THIS
#    ### !!!!!!!!!!!!!!!!!!!!!!!!!
#        query = "INSERT INTO jpegs (domain, tries, data, location, saved) VALUES (%s,%s,%s,%s,%s)"
#        crawlCursor.execute( 
#                            query, (
#                            mysql.escape_string( jpeg['domain'] ),
#                            jpeg['tries'],
#                            mysql.escape_string( data ),
#                            mysql.escape_string( jpeg['location']),
#                            jpeg['saved'] ) 
#                           )
#    except:
#        print jpeg
#    crawlConnection.commit()

mongoLinks = linksCollection.find(timeout=False)

for link in mongoLinks:
    try:
        if not link['contents']:
            link['contents'] = ""
        link['contents'] = link['contents'].encode("utf8")
        if 'checked' not in link.keys():
            link['checked'] = False
        query = "INSERT INTO links (domain, checked, gotten,  visited, location, contents) VALUES (%s,%s,%s,  %s,%s,%s)"
        
        crawlCursor.execute( 
                            query, (
                            link['domain'],
                            link['checked'],
                            link['read'],
                            link['visited'],
                            link['location'],
                            link['contents'])
                            )
        crawlConnection.commit()
    except UnicodeEncodeError as e:
        print e
        print link['contents']
        sys.exit()
        
print "reached end of program"
