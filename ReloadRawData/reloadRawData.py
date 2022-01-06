import os, sys
sys.path.append("%s%s" % (os.getcwd(), os.path.sep))

from datetime import datetime, timedelta
import logging
from Service.logs import setupLogging
from Local.config_local import mysql_credentials as config
import traceback
from Service.emailTool import sendEmailtoAdmin
import platform
import sqlite3, csv

import mysql.connector
from Service.datamodel import mappedDS, session

def getMySQLConnection():
    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        passwd=config['password'],
        database=config['db']
    )


if __name__ == '__main__':
    # This script is mainly used for reloading the raw table records for reasons like new models are added and the old records were already loaded to the big table
    # The data to be reloaded are stored in a CSV file "dataToBeReload.txt"

    # 1) read a CSV into a SQLLite & get a list of ordered source and sourceid for reloading
    con = sqlite3.connect(".memory")
    cur = con.cursor()
    cur.execute('''CREATE TABLE  if not exists data
           (source           TEXT    NOT NULL,
            sourceid         TEXT    NOT NULL);''')
    cur.execute(''' Delete from data ; ''')
    dataFile = open("dataToBeReload.csv", newline='', encoding='utf-8')
    rows = csv.reader(dataFile)
    cur.executemany("INSERT INTO data VALUES (?, ?)", rows)
    cur.execute("SELECT distinct source, sourceid FROM data order by source, sourceid;")
    records = cur.fetchall()
    print("Records: %d" %(len(records)))
    mysqlConnection = getMySQLConnection()
    myCursor = mysqlConnection.cursor()
    for record in records:
        source = record[0]
        sourceId = record[1]
        rawtableName = mappedDS[source].rawTableName
        print(source,sourceId, rawtableName)
        # Only update the latest/oldest record
        myQuery = """
                select distinct id 
                from {rawTable}
                where sourceid = "{sourceId}" and scanTime = (select max(scanTime) from {rawTable} where sourceid ="{sourceId}")
            """
        # print(myQuery.format(rawTable=rawtableName, sourceId=sourceId))
        myCursor.execute(myQuery.format(rawTable=rawtableName, sourceId=sourceId))
        result=myCursor.fetchone()


        myQuery2 = """
                update {rawTable}
                set processed = 0, processedAt = NULL
                where id = {id}
            """
        # print(myQuery2.format(rawTable=rawtableName, id=result[0]))
        myCursor.execute(myQuery2.format(rawTable=rawtableName, id=result[0]))

    mysqlConnection.commit()



