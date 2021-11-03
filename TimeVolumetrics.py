# This app is made to automatically consolidate the time volumetrics of each report, based on the timestamp given within the report
import re
import mysql.connector
import time
from IPython.display import clear_output
from datetime import datetime, timedelta

mydb = mysql.connector.connect(
    host = '127.0.0.1',
    user = 'user',
    password = 'password'
    database = 'test'
)
mycursor = mydb.cursor()

def clear():
    clear_output()
    print("Welcome to the Volumetrics Consolidator by Ben.\n\nThis script is set to run every 10 minutes")

def sleep(a):
    time.sleep(a)

def con_vol()
    print("Running Volumetrics Consolidator...")
    global sec
    # 1. Run query to obtain report which are committed in main db but not updated in volumetrics db
    mycursor.execute("""SELECT reportid, report from test.report r where not exists 
    (SELECT volid from test.volumetrics v where v.reportid = r.reportid) group by r.reportid""")
    reports = mycursor.fetchall()
    for rpt in reports:
        reportid = rpt[0]
        report = rpt[1]
        timestamps = re.findall("\d\d:\d\d:\d\d", str(report))
        ct = set(timestamps)
        ttime = len(ct)
        print("The report ID is" + str(rptid))
        # 2. Query to check if total time exists
        querystmt = f"SELECT * from test.volumetrics where reportid = {reportid} and totaltime = {ttime}"
        mycursor.execute(querystmt)
        querycheck = mycursor.fetchone()
        print(querycheck)
        # 3. Insert total time in respect to each reportid
        if querycheck is None:
            sql = f"INSERT INTO test.volumetrics SET reportid = {reportid}, totaltime = {ttime}"
            mycursor.execute(sql)
            mydb.commit()
        else:
            sql = f"UPDATE test.volumetrics SET transid = {transid}, totaltime = {ttime}"
            mycursor.execute(sql)
            mydb.commit()
    sysstatuss = "UPDATE test.systemstatusmonitor SET lastruntimestamp = %s where sysname %s"
    sysdata = (datetime.now(), 'volumetrics')
    mycursor.execute(sysstat, sysdata)
    mydb.commit()
    clear()
    sec = 0

while True:
    con_vol()
    sleep(600)

#THE END
