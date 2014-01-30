#!/usr/bin/python2.7


# this version deprecated

# this is a script to find links that look like jpeg links... change the regex to find different things, could be better...



import re
from urlparse import urlparse

from pymongo import MongoClient
client = MongoClient()
linksCollection = client.crawl.links
jpegsCollection = client.crawl.jpegs


def insertJpeg(location, conn):
    if conn.find( {"location" : location} ).count() == 0:
        domain = str( urlparse( location ).netloc )
        conn.insert( { "domain":domain,
                       "location":location,
                       "saved":False,
                       "tries":0 } )

for doc in linksCollection.find( {"Checked": None }):
    if not doc:
        continue
    doc['checked'] = True
    linksCollection.save(doc)
    if not doc['contents']:
        continue
    jpegs = re.findall(r'https?://[^">]*?\.jpg', doc['contents'])
    for x in jpegs:
        insertJpeg(x, jpegsCollection)
    
