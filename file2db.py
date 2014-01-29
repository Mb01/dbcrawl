#!/usr/bin/python2.7

import os

import MySQLdb as mysql

#could change hard coded to argparse
os.chdir('pics')


HOST = "localhost"
USER = 'testuser'
PASSWORD = 'test623'

conn = mysql.connect(host=HOST,user=USER,passwd=PASSWORD, db='testdb')# database name here

cursor = conn.cursor()

# DROP A TABLE! REALLY?
# Will wipe all data!
#
# query = DROP TABLE IF EXISTS Images #add quotes to use

#cursor.execute( query )


# No table? Run this.
#
#query = """CREATE TABLE IF NOT EXISTS Images 
#                (Id INT PRIMARY KEY AUTO_INCREMENT,
#                data MEDIUMBLOB,
#                time VARCHAR(30),
#                filename VARCHAR(200))
#                """
#cursor.execute( query )

list_of_pics = os.listdir( os.getcwd() )

#make a dictionary of modified time in ctime
time_modified = {} 
for filename in list_of_pics:
    time_modified[filename] = os.path.getctime( os.path.join(  os.getcwd(), filename  ) )


for filename in list_of_pics:
    try:
        data = open(filename, "rb").read()
        time = time_modified[filename]
        query = "INSERT INTO Images (data, time, filename) VALUES ('%s','%s','%s')" % \
                ( mysql.escape_string(data),
                  str(time), 
                  mysql.escape_string(filename) )
        
        cursor.execute( query )
        conn.commit()
    except Exception as e:
        print e

