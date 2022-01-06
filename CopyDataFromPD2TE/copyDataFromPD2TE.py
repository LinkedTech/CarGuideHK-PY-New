import sys,os
sys.path.append("%s%s" % (os.getcwd(), os.path.sep))
from Local.config_local import mysql_credentials
import mysql.connector
from CopyDataFromPD2TE.config_copyDataFromPD2TE import RawTables, ListTables

dbConnection = mysql.connector.connect(
    host=mysql_credentials["host"],
    user=mysql_credentials["user"],
    passwd=mysql_credentials["password"],
    database=mysql_credentials["db"])

dbPD = "carguidehk-prod"
dbTE = "carguidehk"
dbCursor = dbConnection.cursor()


def copyListTables(days):
    for listTable in ListTables:
        print(listTable)
        myQuery = """
                INSERT INTO `{dbTE}`.`{listTable}`  
                SELECT * FROM `{dbPD}`.`{listTable}` 
                where scanTime > SUBSTRING(TIMESTAMP(NOW() - INTERVAL {day} DAY), 1, 10); """
        # print(myQuery.format(dbPD=dbPD, dbTE=dbTE, listTable=listTable, day=days))
        try:
            dbCursor.execute(myQuery.format(dbPD=dbPD, dbTE=dbTE, listTable=listTable, day=days))
        except:
            print("Problem: %s" %(listTable))
    dbConnection.commit()


def copyRawTables(hours):
    for rawTable in RawTables:
        print(rawTable)
        myQuery = """
                INSERT INTO `{dbTE}`.`{rawTable}`  
                SELECT * FROM `{dbPD}`.`{rawTable}` 
                where scanTime > SUBSTRING(TIMESTAMP(NOW() - INTERVAL {hour} HOUR), 1, 13); """
        print(myQuery.format(dbPD=dbPD, dbTE=dbTE, rawTable=rawTable, hour=hours))
        try:
            dbCursor.execute(myQuery.format(dbPD=dbPD, dbTE=dbTE, rawTable=rawTable, hour=hours))
            myQuery2 = """
                    UPDATE `{dbTE}`.`{rawTable}`  
                    SET processed=0, processedAt=NULL
                    where scanTime > SUBSTRING(TIMESTAMP(NOW() - INTERVAL {hour} HOUR), 1, 13); """
            print(myQuery2.format(dbPD=dbPD, dbTE=dbTE, rawTable=rawTable, hour=hours))
            dbCursor.execute(myQuery2.format(dbTE=dbTE, rawTable=rawTable, hour=hours))
        except:
            print("Problem: %s" %(rawTable))

    dbConnection.commit()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: Command SenderMobileNumber, Command = 1) Copy raw tables hourly | 2) Copy list table daily ")
        command = input("Enter command: ").lower()
    else:
        command = sys.argv[1].lower()
    if command == "1":
        copyRawTables(0)
    elif command == "2":
        copyListTables(0)
