#!/usr/bin/python2.7




# THIS ISN'T A SO USEFUL PROGRAM BUT IT WORKS WITH manualsave.py 

from pymongo import MongoClient

client = MongoClient()
jpegsCollection = client.crawl.jpegs


fil = open( "jpgs.txt", "a" )
toLoad = []
for jpeg in jpegsCollection.find( {"saved" : False}):      
    fil.write(jpeg['location'])
    fil.write("\n")
