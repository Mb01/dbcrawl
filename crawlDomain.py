#!/usr/bin/python2.7

#   follow links, stay in domain save links

from argparse import ArgumentParser
import sys
from urllib2 import urlopen, URLError, HTTPError
from urlparse import urlparse
import time
from bs4 import BeautifulSoup as bs
import re
import httplib


from pymongo import MongoClient, ASCENDING
client = MongoClient()
linksCollection = client.crawl.links
jpegsCollection = client.crawl.jpegs


#   Mongo db functions

def insertLink(location, connection):
    if connection.find( {"location" : location} ).count() == 0:
        domain = str( urlparse( location ).netloc )
        connection.insert( { "domain" : domain,
                              "location": location,
                              "contents": None,
                              "visited": False,
                              "read": False, 
                              "links": []} )

def insertJpeg(location, connection):
    if connection.find( {"location" : location} ).count() == 0:
        domain = str( urlparse( location ).netloc )
        connection.insert( { "domain":domain,
                             "location":location,
                             "saved":False,
                             "tries":0 } )
    
def ensureIndexes():
    #ensure some indexes
    linksCollection.ensure_index([("visited", ASCENDING)] , background=True, cache_for=1000)
    linksCollection.ensure_index([("location", ASCENDING)] , background=True, cache_for=100)
    jpegsCollection.ensure_index([("location", ASCENDING)] , background=True, cache_for=100) 

def try_utf8(data):
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        print "Couldn't decode as utf-8"
        return None        


###############################################################################################
###############################################################################################
###############################################################################################
###############################################################################################

def getLinks(collection):
    return collection.find({"visited":True, "domain":DOMAIN}, limit=1000)


def fetchLink( link ):
    print "trying %s" % link['location']    
    try:
        link['visited'] = True 
        contents = try_utf8( urlopen( link['location'] ).read() )
        link['contents'] = contents
        link['read'] = True
        linksCollection.save( link )
    except (IOError, URLError, HTTPError, ValueError, AttributeError, httplib.IncompleteRead) as e:
        print "Couldn't read %s because of %s" % (link['location'],str(e))
        link['contents'] = None
        link['read'] = False
        link['visited'] = True                
        linksCollection.save( link )
    return link




#   IS THERE A SEED
parser = ArgumentParser()
parser.add_argument("seed", help="a url to start from")
args = parser.parse_args()
seed = args.seed
DOMAIN = str( urlparse( seed ).netloc )
if linksCollection.find( {"visited" : False, "domain" : DOMAIN}, limit=1000):
    insertLink(seed, linksCollection)

#   let's go

while linksCollection.find_one( {"visited" : False, "domain" : DOMAIN}):

        ensureIndexes()
        for link in getLinks( linksCollection ):
            link = fetchLink( link )
            if not link['contents']:
                continue
            #FIND THE LINKS
          
            soup = bs( link['contents'] )

            for a in soup.find_all('a'):
                href = a.get('href')
                if not href: continue
                try:
                    href = href.encode('utf-8')
                    if href[-1] == '/':
                        href = href[:-1]
                    if str(href).find('http') == 0:
                        pass               
                        # do nothing
                    elif str(href).find('/') == 0:
                        # change the href
                        href = "http://" + str( urlparse( href ).netloc ) + "/" + str(href)                
                    else:
                        continue
                    if re.match( r'.*\.jpg',  href ):
                        #print "Inserting %s into jpegs" % href
                        insertJpeg(href, jpegsCollection)
                    else:
                        #print "Inserting %s into links" % href
                        insertLink(href, linksCollection)                       
                except UnicodeError as e:
                    print e





