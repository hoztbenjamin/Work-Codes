# Continuously searches for XML files in folder created, archives into directory based on metadata
# Adds into DB, if error faced, update error into error DB
# If successfully committed into DB, XML file deleted
import itertools
import re
import os
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import tostring
import mysql.connector
from mysql.connector import errorcode
import errno
from getpass import getpass
from mysql.connector import connect, Error
import time
import shutil
import pandas as pd

# Class for datetime storage
class dt:
    def __init__(self, year, month, day, time)
        self.y = year
        self.m = month
        self.d = day
        self.t = time

# Function for splitting date/time
def datetimesplit(dt):
    # Format: YYYYMMDDHHMMSS
    y = dt[:4]
    m = dt[4:6]
    d = dt[6:8]
    t = dt[8:]
    global sd
    sd = f"{y}-{m}-{d}" # Starting Date
    global st
    st = t # Starting time

# Function to add a minute at last timestamp within report
def minuteadd(t):
    hr = t[:3]
    minute = t[3:5]
    sec = t[5:9]
    if int(minute) == 59: # Accounting for end of hour (59 min - 00 min)
        minadd = "00"
        hradd = hr[:2]
        hradd = int(hradd) + 1
        hr = str(hradd) + ":"
    else:
        minadd = int(minute) + 1
    if hr == "24:": # Accounting for end of day (24H becomes 00H)
        hr = "00:"
    global ltime
    ltime = hr + str(minadd) + sec

# File scanning function
def get_filenames(directory):
    file_name = [] # Lists to store filenames
    for root, directories, files, in os.walk(directory): 
        for filename in files:
            file_name.append(filename)
    return file_name

# Folder creation function
def createfolder(directory):
    try:
        os.makedirs(directory) # Try creating directory
    except OSError as e:
        if e.errno != errno.EEXIST: # If directory exists, fails
            raise
        print("Creation of directory %s failed. Directory already exists" %directory)
    else:
        print("Successfully created the directory %s" %directory)

# SQL query function (searching for reportid)
def get_reportid(date,time): # ID is based on date and time provided
    id = []
    try:
        connection = mysql.connector.connect(user= 'user', password = 'password',
                                            host = '127.0.01',
                                            database = 'test') # Insert DB info here
        cursor = connection.cursor()
        reportid = f"""SELECT reportid from test.report
                    where startdate = {date} and starttime = {time}
                    """ # Search for relevant ID
        cursor.execute(reportid)
        records = cursor.fetchall()
        for row in records:
            id.append(row[0]) # Only require the first output corresponding to date/time
        if id == []:
            return 0 # If doesn't exist = 0 
        else:
            return int(f'{id[0]}')
            print("The report ID is: {id}")
    except mysql.connector.Error or IndexError as error:
        print(error)
    finally:
        if connection.is_connected(): # Close SQL connection after all is done
            cursor.close()
            connection.close()
            print("SQL connection closed")

# Error logging function   
def errormove(error, errfile):
    print(error)
    destination = "C:/Desktop/Error Files"
    os.chdir(destination)
    col = ["Errorfile", "Error"]
    errorfile.append(errfile)
    error.append(error)
    row = ()
    erpd = pd.DataFrame(row, columns = col) # Format in pandas
    erpd["Errorfile"] = errorfile
    erpd["Error"] = error
    erpd.to_csv("ErrorLog.csv", sep = "\t") # Export out in tab separator format
    shutil.move(full_filename,os.path.join(destination, errfile))
    print(f"Moved file to {destination}\n*************************\n\n")

# List of error arrays to store errors while program runs
error = []
errorfile []

# Program codes (runs infinitely to process XML files created)
while 1:
    XMLpath = "C:/XMLfiles"
    os.chdir(XMLpath)
    files = get_filenames(XMLpath)
    err = 0
    immediate = []
    for file in files:
        update = 0
        full_filename = os.path.join(XMLpath, file) 
        print(full_filename)
        # Use ElementTree to parse XML files into respective metadata/content
        try:
            parser = ET.XMLParser(encoding = 'utf-8')
            time.sleep(2)
            tree = ET.parse(full_filename, parser = parser)
            root = tree.getroot() # Read parsed XML
            for child in root:
                if child.tag == 'HEAD':
                    for header in child:
                        if header.tag == "MODE":
                            md = header.text
                        if header.tag == "REPORTBY":
                            rb = str(header.text)
                        if header.tag == "CASENOTA":
                            cn = header.text
                        if header.tag == "KEYWORD":
                            pattern = r"[|]" 
                            mod_string = re.sub(pattern, '"', str(header.text))
                            key = mod_string
            for child in root:
                if child.tag == 'BODY':
                    for tt in child:
                        if tt.tag == "STARTDATE"
                            stdate = tt.text
                            x = dt(tt.text[:4], tt.text[4:6], tt.text[6:8], tt.text[8:])
                            datetimesplit(str(tt.text))
                            stdate = sd
                            sttime = st
                        if tt.tag == "ENDDATE"
                            enddate = tt.text
                            datetimesplit(str(tt.text))
                            enddate = sd
                            endtime = st
                        if tt.tag == "REMARKS":
                            pattern = r"[|']"
                            mod_string = re.sub(pattern, '"', str(ta.text))
                            rmk = mod_string
                        if tt.tag == "REPORT"
                            rpt = str(tt.text)
                            pattern = r"[|']"
                            pattern1 = r"[;]"
                            mod_string = re.sub(pattern,'"', str(tt.text))
                            mod_string1 = re.sub(pattern1, ':', str(mod_string))
                            timestamps = re.findall("\d\d:\d\d:\d\d:", str(mod_string1))
                            if timestamps == []:
                                print("No timestamp given")
                                errormove("No timestamp given or timestamp in wrong format", file)
                                err = 1
                            else:
                                firsttime = timestamp[0]
                                lasttime = timestamp[-1]
                                minuteadd(lasttime)
                                lasttime = ltime
                            immediate = re.findall("|S.+\n\S.+\nF/.+", str(mod_string1))
                            print(immediate)
                if err = 1:
                    break
                path = f"C:/Desktop/Reports/{x.y}/{x.m}/{x.d}" 
                print(path)
                createfolder(path)
                time.sleep(1)
                os.chdir(path)
                try:
                    with open("%s.txt"%cn, "w") as z:
                        z.write(str(fulltext))
                    print("Successfully created the file: %s.txt\n\n"%cn)
                except NotADirectoryError as e:
                    print(f"Error occured with file '{file}'")
                    print(e)
                    errormove(e, file)
                    break
                time.sleep(1)
                # Update Field
                rptid = get_reportid(startdate, starttime)
                print(f"The report ID is : {rptid}.")
                if rptid == 0:
                    update = 0
                else:
                    update = 1
                    print("This is an update.\n\n\n\n")
                if update == 1:
                    try:
                        with connect(user = 'user', password = 'password',
                                    host = '127.0.01', database = 'test')
                        as connection:
                            update_query = f"""UPDATE test.report SET
                            mode = '{md}', reportby = '{rb}', casenota = '{cn}', keyword = '{key}',
                            startdate = '{startdate}', startime = '{startime}, 
                            enddate = '{enddate}, endtime = '{endtime}',
                            report = '{mod_string1}',
                            ftime = '{ftime}', ltime = '{ltime}
                            WHERE ('reportid' = '{reportid}')"""
                            with connection.cursor() as cursor:
                                cursor.execute(update_query)
                                connection.committed
                                print("Successfully commited into database")
                        os.chdir(XMLpath)
                    except Error as e:
                        errormove(e, file)
                        break
                if update == 0:
                    try:
                        with connect(user = 'user', password = 'password',
                                    host = '127.0.01', database = 'test')
                        as connection:
                            insert_report = f"""INSERT INTO report (mode, reportby, casenota, startdate,
                             starttime, enddate, endtime, report, remarks, keyword , xmlid, ftime, ltime) VALUES
                             ('{md}','{rb}','{cn}','{startdate}','{starttime}','{enddate}','{endtime}','{rpt}','{rmk}'
                             ,'{key}','{file}','{firsttime}','{lasttime}')"""
                            with connection.cursor() as cursor:
                                cursor.execute(insert_report)
                                connection.commit()
                                print("Successfully committed into REPORT database")
                            os.chdir(path_to_watch)
                            rptid = get_reportid
                            print("The report id is: '{rptid}'")
                    except Error as e:
                        errormove(e, file)
                        break
                    # Handling immediates    
                    for immediates in immediate:
                        y = immediates.split('\n')
                        itime = y[0]
                        fm = y[1]
                        finfo = y[2][2:]
                        print("The immediate time is {itime}, from {fm}, the immediate info is {finfo}")
                        # Insert into db
                        try:
                            with connect(user = 'user', password = 'password',
                                        host = '127.0.0.1', database = 'test')
                            as connection:
                            insert_immediate = f"""INSERT INTO immediate (itime, fm, finfo, reportid) 
                                                VALUES ('{itime}','{fm}','{finfo}','{reportid}')"""
                            with connection.cursor() as cursor:
                                cursor.execute(insert_immediate)
                                connection.commit()
                                print("Successfully commited into IMMEDIATE database")
                        except Error as e:
                            errormove(e,file)
                            err = 1
                            break
                if err = 1:
                    break
                try:
                    os.remove(full_filename)
                    print("Root file removed")
                except PermissionError or FileNotFoundError or Error as e:
                    print(f"Error occured with file '{file}'")
                    print(e)
                    errormove(e, file)
                    print(f"{file} is being accessed or locked, please close/unlock the file\n\n\n")
                    continue
            except Error as e:
                if e != shutill.Error or NotADirectoryError:
                    errormove(e, file)
                else:
                    destination = "W:/Desktop/Unprocessed Files"
                    os.chdir(destination)
                    col = ["Errorfile", "Error"]
                    errorfile.append(file)
                    error.append(e)
                    row = ()
                    erpd = pd.DataFrame(row, columns = col)
                    erpd["Errorfile"] = errorfile
                    erpd["Error"] = error
                    erpd.to_csv("Errorlog.csv", sep = "\t")
                    continue
        time.sleep(2)
# THE END
                
                





                        
                            
                        
                            
        
