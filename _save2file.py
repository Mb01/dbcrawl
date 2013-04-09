#!/usr/bin/python2.7

# THIS JUST SAVES FILES TO DISK


import argparse
import urllib
from urllib2 import urlopen, URLError, HTTPError
from urlparse import urlparse
import os
import httplib
import time

from pymongo import MongoClient, ASCENDING

client = MongoClient()
jpegsCollection = client.crawl.jpegs


"""
import mechanize
import cookielib
br = mechanize.Browser()
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)
br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(True)
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)
#br.set_debug_http(True)
#br.set_debug_redirects(True)
#br.set_debug_responses(True)
br.addheaders = [('Python2.7')]
"""

SAVEPATH = "pics/"

#   take input
def savePic( url ): #fileSystemVersion
    url = url.encode('utf8')
    p = urlparse( url )
    filestring = str( p.netloc + p.path ).replace('/','%' )
    if os.path.isfile( SAVEPATH + filestring ):
        return SAVEPATH + filestring
    print "trying %s" % url  
    pic = urlopen( url ).read()
    if len(pic) < 200:
        return None
    picFile = open(SAVEPATH + filestring , "w" )
    picFile.write( pic )
    picFile.close()
    return filestring

while True:
    jpegsCollection.ensure_index([("saved", ASCENDING)] , background=True, cache_for=600)
    toLoad = jpegsCollection.find( {"saved" : False, "tries" : 0 }, limit=300 )
    sleepTime = 2    
    if toLoad.count() == 0:
        toLoad = jpegsCollection.find( {"saved" : False, "tries" :{ '$lt':2 } }, limit=50 )
        sleepTime = 15
    for jpeg in toLoad:
        try:
            location = savePic( jpeg['location'] )
            jpeg['local_location'] = location
            jpeg['saved'] = True
            jpeg['tries'] += 1
            jpegsCollection.save(jpeg)
        except (IOError, URLError, UnicodeError, HTTPError, ValueError, AttributeError,httplib.IncompleteRead) as e:
            jpeg['tries'] += 1
            jpegsCollection.save(jpeg)
            print e
        time.sleep(sleepTime)
    time.sleep(sleepTime * 10)

