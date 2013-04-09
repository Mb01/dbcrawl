#!/usr/bin/python2.7


# I could make this way more generic


import os

import MySQLdb as mysql

########################
#####
########################
DIR = "pics"
# so you need a pics dir
# os.chdir('pics')

# so you need a dir DIR
os.chdir(DIR)

# could change hard coded to argparse
# os.chdir('pics') #if you want to put pics back where they came from
HOST = "localhost"
USER = 'testuser'
PASSWORD = 'test623'

conn = mysql.connect( host=HOST,user=USER,passwd=PASSWORD, db='testdb' )

cursor = conn.cursor()

query = "SELECT * FROM Images WHERE Id >= %s AND Id < %s"
savedir = ""
counter = 0

 # result[1] is data
 # ...0 Id ... 2 time ... 3 filename
    # print "%s %s %s" % (result[0], result[2], result[3])
    # >> 1 1364752105.38 blog-imgs-47.fc2.com%k%a%g%kagonekoshiro%t10090324.jpg
    #    Id time         filename
minId = 0
maxId = 1000


while True:
    cursor.execute( query, ( minId, maxId ) )
    minId += 1000
    maxId += 1000
    results = cursor.fetchall()
    if not results:
        break
    for result in results:
        if counter % 300 == 0:
            savedir = str( counter )
            os.mkdir( savedir )
        fil = open(os.path.join(savedir, result[3]), "wb")
        fil.write(result[1])
        fil.close()
        counter += 1
