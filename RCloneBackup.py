from argparse import ArgumentParser
from pathlib import Path
import sqlite3
import re
import datetime
import configparser
import subprocess
import json
import os
import smtplib
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def SetTaskInfo(database,taskID,lastUpdate):
    conn = create_connection(database)
    dataValue = (taskID, lastUpdate)
    UpdateTaskInfo(conn, dataValue)

def GetTaskInfo(database, taskID):
    TaskInfo=''
    if not os.path.exists(database):
        conn = create_connection(database)
        if conn is not None:
            sql_crate_table = """ CREATE TABLE IF NOT EXISTS TASK_INFO (
                                                ID text NOT NULL UNIQUE,
                                                LAST_UPDATE DATETIME NOT NULL
                                                ); """

            create_table(conn, sql_crate_table)
        else:
            print("Error! cannot create the database connection.")
    else:
        conn = create_connection(database)

    dataValue = (taskID, datetime.datetime.now())
    queryResult = SelectTaskInfo(conn, taskID)
    if not queryResult:
        InsertTaskInfo(conn, dataValue)
        queryResult = SelectTaskInfo(conn, taskID)

    return queryResult

def SelectTaskInfo(conn, taskID):
    cur = conn.cursor()
    cur.execute('SELECT * FROM TASK_INFO WHERE ID = "' + str(taskID) + '";')
    rows = cur.fetchall()
    return rows

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except OSError as e:
        print(e)
    return None

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except OSError  as e:
        print(e)


def InsertTaskInfo(conn, value):
    sql = 'INSERT INTO TASK_INFO(ID, LAST_UPDATE) VALUES("' + str(value[0]) + '","' + str(value[1]) + '");'
    cur = conn.cursor()

    try:
        cur.execute(sql)
    except sqlite3.IntegrityError as e:
        print('sqlite error: ', e.args[0])  # column name is not unique
    conn.commit()
    return cur.lastrowid

def UpdateTaskInfo(conn, value):
    sql = 'UPDATE TASK_INFO SET LAST_UPDATE = "' + str(value[1]) + '" WHERE ID = "' + str(value[0])+ '";'
    cur = conn.cursor()

    try:
        cur.execute(sql)
    except sqlite3.IntegrityError as e:
        print('sqlite error: ', e.args[0])  # column name is not unique
    conn.commit()
    return cur.lastrowid


def ReturnFolderName(path):
    return os.path.basename(path)

def MakeFolder(path):
    RCloneRunCommand('mkdir ' + str(path))

def DeleteFolder(path):
    RCloneRunCommand('purge ' + str(path))

def GetFolderList(path):
    rvalue=[]
    RCloneReturn = RCloneRunCommand('lsjson ' + str(path))
    RCloneReturn[0] = ''.join(RCloneReturn[0])
    vjson = json.loads(RCloneReturn[0])
    for element in vjson:
        if element['MimeType'] == 'inode/directory':
            rvalue.append(element['Name'])
    return rvalue

def RCloneRunCommand(command):
    objListValue=[]
    process = subprocess.Popen(str(Path().absolute()) + '//rclone.exe ' + command , stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    while True:
        stdoutValue = process.stdout.readline()
        stderrValue = process.stderr.readline()
        if (stdoutValue == '' or stdoutValue == b'') and (stderrValue == '' or stderrValue == b'') and process.poll() is not None:
            break
        if stdoutValue:
            objListValue.append(str(stdoutValue.strip(), "UTF-8"))
        if stderrValue:
            objListValue.append(str(stderrValue.strip(), "UTF-8"))
    return [objListValue,process.returncode]

def DoesTheFolderExist(localToCheck, folderName):
    folderList = GetFolderList(localToCheck)
    for folder in folderList:
        if str(folderName) == folder:
            return True
    return False

def CheckFileStructDB(path,fileName):
    RCloneReturn = RCloneRunCommand('lsjson ' + str(path))
    RCloneReturn[0] = ''.join(RCloneReturn[0])
    vjson = json.loads(RCloneReturn[0])
    for element in vjson:
        if (element['MimeType'] == 'application/octet-stream') and (element['Name'] == fileName ):
            return True
    return False

def ClearOldBackups(diffsToRetain, path, folderName):
    rvalue=[]
    RCloneReturn = RCloneRunCommand('lsjson ' + str(path))
    RCloneReturn[0] = ''.join(RCloneReturn[0])
    vjson = json.loads(RCloneReturn[0])
    for element in vjson:
        if (element['MimeType'] == 'inode/directory') and ( str(element['Name']).startswith(folderName + '-[')):
            rvalue.append(element['Name'])

    if rvalue.__len__() > (diffsToRetain + 1):
        toDelete = (rvalue.__len__() - (diffsToRetain + 1))
        count = 1
        while count <= toDelete:
            teste = (sorted(rvalue)[count - 1])
            RCloneReturn = RCloneRunCommand('purge ' + str(path / (sorted(rvalue)[count-1])))
            count += 1

def IsTheFolderEmpty(path):
    rvalue = False
    RCloneReturn = RCloneRunCommand('lsjson ' + str(path))
    RCloneReturn[0] = ''.join(RCloneReturn[0])
    vjson = json.loads(RCloneReturn[0])
    if vjson == []:
        rvalue = True
    return rvalue

def SendEmail(email_server,email_port,from_addr,password,to_addr,subject,content):
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject
    body = MIMEText(content, 'html')
    msg.attach(body)

    server = smtplib.SMTP(email_server, email_port)
    server.login(from_addr, password)
    server.send_message(msg, from_addr=from_addr, to_addrs=[to_addr])

def getRawGotStr(s):
    return s.encode('unicode-escape').decode()

def WriteLog(path,data):
    with io.open(Path(path), 'a', encoding='utf8') as f:
        f.write(data + '\n')
        f.close()

def SaveFolderStruct(database, directory):
    if not os.path.exists(database):
        conn = create_connection(database)
        if conn is not None:
            sql_crate_table = """ CREATE TABLE IF NOT EXISTS STRUCT (
                                                IS_FOLDER boolean NOT NULL,
                                                PATH text NOT NULL UNIQUE
                                                ); """

            create_table(conn, sql_crate_table)
        else:
            print("Error! cannot create the database connection.")
    else:
        conn = create_connection(database)

    for root, dirs, files in os.walk(directory):
        path = root.split(os.sep)
        dataValue = (os.path.isdir(root), os.path.basename(root))
        InsertFolderStruct(conn,dataValue)
        for file in files:
            dataValue = (os.path.isdir(Path(Path(root) / file)), os.path.join(os.path.basename(root),file))
            InsertFolderStruct(conn,dataValue)


def InsertFolderStruct(conn, value):
    sql = 'INSERT INTO STRUCT(IS_FOLDER, PATH) VALUES (' + str(int(value[0])) + ',"' + str(value[1]) + '");'
    cur = conn.cursor()

    try:
        cur.execute(sql)
    except sqlite3.IntegrityError as e:
        print('sqlite error: ', e.args[0])  # column name is not unique
    conn.commit()
    return cur.lastrowid

def RunTask(taskFile):
    error = 0
    Messages = []
    Messages.append("Resume of the job:")
    now = datetime.datetime.now()
    startTime= now
    Messages.append("Started on: " + startTime.strftime('%d/%m/%Y %H:%M:%S'))
    today = now.strftime('%Y-%m-%d-%H-%M-%S')

    configParser = configparser.RawConfigParser()
    configParser.read(taskFile)
    taskID = int(configParser.get('TASK', 'id'))
    taskName = str(configParser.get('TASK', 'name'))
    taskFolders = Path(configParser.get('TASK', 'folders'))
    taskDiff = int(configParser.get('TASK', 'diff'))
    taskEmailServer = str(configParser.get('TASK', 'email-server'))
    taskEmailPort = str(configParser.get('TASK', 'email-port'))
    taskEmailUser = str(configParser.get('TASK', 'email-user'))
    taskEmailPassword = str(configParser.get('TASK', 'email-password'))
    taskEmailTo = str(configParser.get('TASK', 'email-to'))

    taskInfoDB = Path(Path().absolute() / 'tasks.sqlite3')
    logName = re.sub('[ \\\\/:"*?<>|]', '', str(taskName + '-' + str(taskID) + '-' + today))
    firstTime = 0


    #check if log folder exist. if not create folder
    if not DoesTheFolderExist(str(Path(Path().absolute())), 'Logs'):
        MakeFolder(str(Path(Path().absolute() / 'Logs' )))

    #def log file
    logFile= str(Path(Path().absolute() / 'Logs' / str(logName +'.log')))

    LastDataSent = GetTaskInfo(taskInfoDB, taskID)

    WriteLog(logFile, "Started on: " + startTime.strftime('%d/%m/%Y %H:%M:%S'))

    #check if all source and destination folders exists
    folders = str(taskFolders).split('|')
    for folder in folders:
        FolderPair = str(folder).split('->')

        if not os.path.exists(str(FolderPair[0]).strip()):
            error=1
            Messages.append('<strong>The source folder ' + str(FolderPair[0]).strip() + ' does not exist.</strong>')

        if not DoesTheFolderExist(str(Path(str(FolderPair[1]).strip()).parent), str(ReturnFolderName(str(FolderPair[1]).strip()))):
            error=1
            Messages.append('<strong>The destination folder ' + str(FolderPair[0]).strip() + ' does not exist.</strong>')

    #if no error start send data processs
    if error == 0:
        folders = str(taskFolders).split('|')
        for folder in folders:
            WriteLog(logFile, "=============================================================================")
            WriteLog(logFile, "Synchronizing " + getRawGotStr(folder))
            FolderPair = str(folder).split('->')
            taskSource = Path(FolderPair[0].strip())
            taskDestination = Path(FolderPair[1].strip())

            folderName = ReturnFolderName(taskSource)
            label = datetime.datetime.strptime(LastDataSent[0][1], '%Y-%m-%d %H:%M:%S.%f')
            label = label.strftime('%Y-%m-%d-%H-%M-%S')

            folderStructDBName = re.sub('[ \\\\/:"*?<>|]', '', str(folderName + '-' + str(taskID) + '-'+ today + '.str'))
            folderStructDB = Path().absolute() / folderStructDBName


            destFull = taskDestination / (folderName + '-[FULL]')
            destDiff = taskDestination / (folderName + '-[' + label + ']')

            #save folder struct to be possible restore files
            SaveFolderStruct(folderStructDB, taskSource)

            #create full folder on dest if not exists
            if not DoesTheFolderExist(taskDestination, destFull):
                firstTime=1
                MakeFolder(destFull)

            # create diff folder on dest if not exists
            if not DoesTheFolderExist(taskDestination, destDiff):
                MakeFolder(destDiff)

            #send data to destination
            RCloneReturn = RCloneRunCommand('sync "' + str(taskSource) + '" "' + str(destFull) + '" --backup-dir "' + str(destDiff) + '" -v"')

            if RCloneReturn[1] == 0:
                #check if have any file modified on diff folder of this running task
                if IsTheFolderEmpty(str(destDiff)):
                    if firstTime == 1:
                        RCloneRunCommand('copy "' + str(folderStructDB) + '" "' + str(destFull) + '"')


                    if CheckFileStructDB(destFull, folderStructDBName):

                        Messages.append('<br>No changes on data.<br>')
                        os.remove(folderStructDB)
                    else:
                        error = 1
                        Messages.append('Cannot locate struct folder data base on application directory.')

                    DeleteFolder(str(destDiff))
                else:
                    RCloneRunCommand('copy "' + str(folderStructDB) + '" "' + str(destFull) + '"')

                    if CheckFileStructDB(destFull, folderStructDBName):
                        SetTaskInfo(taskInfoDB, taskID, startTime)
                        Messages.append('<br>The data was updated successfully.<br>')
                        os.remove(folderStructDB)
                        # clean old backup folders based on task config
                        ClearOldBackups(taskDiff, taskDestination, folderName)
                    else:
                        error = 1
                        Messages.append('Cannot locate struct folder data base on destination.')
            else:
                error = 1
                Messages.append('<strong  style="font-size: 18px; color: #ff0000;"><br>The task was finished with some problems. Please check the logs to get a details of the errors.</strong><br>')

            #write rclone stdout and sterr to log file
            for line in RCloneReturn[0]:
                WriteLog(logFile, line)

    if error == 0:
        backupStatus = 'Status: Success'
    else:
        backupStatus = 'Status: Failure'

    subject = str(taskName).upper() + "|" + backupStatus
    endTime = datetime.datetime.now()
    elapsedTime = endTime - startTime

    if backupStatus == 'Status: Success':
        Messages.append('<div style="color: blue"><b>'+ backupStatus + '</b></div>')
    else:
        Messages.append('<div style="color: red"><b>' + backupStatus + '</b></div>')

    Messages.append("<b>Started on:</b> " + startTime.strftime('%d/%m/%Y %H:%M:%S'))
    Messages.append("<b>Finished on:</b> " + endTime.strftime('%d/%m/%Y %H:%M:%S'))
    Messages.append("<b>Time elapsed:</b> " + str(elapsedTime))

    WriteLog(logFile, "=============================================================================")
    WriteLog(logFile, "Started on: " + startTime.strftime('%d/%m/%Y %H:%M:%S'))
    WriteLog(logFile, "Finished on: " + endTime.strftime('%d/%m/%Y %H:%M:%S'))
    WriteLog(logFile, "Time elapsed: " + str(elapsedTime))

    emailContent = '<html><body><div style="font-family: Verdana; font-size: 12px">'
    for line in Messages:
        emailContent = emailContent + line + "<br>"

    emailContent = emailContent + "</div></body></html>"
    SendEmail(taskEmailServer, taskEmailPort, taskEmailUser, taskEmailPassword, taskEmailTo, subject, emailContent)

def Main():
    parser = ArgumentParser(description = 'Backup with RClone made by Andrei Bernardo Simoni.')
    parser.add_argument('-t', '--task', action='store', required = True ,dest='taskFile', default='Backup with RClone made by Andrei Bernardo Simoni.', help='Task configuration file to run.')
    arguments = parser.parse_args()
    if os.path.exists(arguments.taskFile):
        RunTask(arguments.taskFile)
    else:
        print('The task file does not exist.')

Main()

