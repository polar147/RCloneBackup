from argparse import ArgumentParser
from pathlib import Path
import sqlite3
import re
import datetime
import configparser
import subprocess
import json
import platform
import os
import smtplib
from smtplib import SMTPException
import io
import ctypes, sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def SetTaskInfo(database,taskID,lastUpdate):
    conn = CreateConnection(database)
    dataValue = (taskID, lastUpdate)
    UpdateTaskInfo(conn, dataValue)

def GetDriveLetter(path):
    rgx = re.match('^[a-zA-Z]{1}:\\\\', str(path))
    return  rgx.group().replace("\\", "")

def GetMappedDrivers():
    currentDir = os.path.dirname(os.path.realpath(__file__))
    return str(RunVBScript(str(Path(currentDir) / 'MapedDrivers.vbs'),""), 'utf-8').replace("}\r\n","}")

def Is64Windows():
    return 'PROGRAMFILES(X86)' in os.environ

def VSSCreate(path):
    rvalue = VSSEasy('CreateShadowCopy "' + path + '"')
    return rvalue

def VSSDelete(ShadowID):
    rvalue = VSSEasy("DeleteShadowCopyByID " + ShadowID)
    return rvalue

def VSSMount(ShadowID, PathToMount):
    rvalue = VSSEasy('MountShadowCopy ' + ShadowID + ' "' + PathToMount + '"')
    return rvalue

def VSSUnmount(path):
    rvalue = VSSEasy('UnmountShadowCopy "' + path + '"')
    return rvalue

def RunVBScript(script, parameters):
    Command = 'cscript /nologo "' + script + '" ' + parameters
    process = subprocess.Popen(Command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    return out + err

def VSSEasy(parameters):
    os.path.abspath(__file__)
    exe = 'VSSEasy32.exe'
    if Is64Windows:
        exe = 'VSSEasy64.exe'
    path = str(Path(Path().absolute() / exe))
    Command = path + ' ' + parameters
    process = subprocess.Popen(Command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = process.communicate()
    return out[0].decode("utf-8")

def AvoidRCloneBug(path):
    #if path is root letter rclone dont work correct
    #fix rclone bug when path is c:\ or other root windows path
    if re.match('^[a-zA-Z]{1}:\\\\$', str(path)) is None:
        rvalue = '"' + path + '"'
    else:
        rvalue = path
    return rvalue

def ListFolderContents(path):
    RCloneReturn = RCloneRunCommand('lsjson ' + AvoidRCloneBug(path) + ' -L')
    rvalue = ''.join(RCloneReturn[0].decode("utf-8"))
    return rvalue

def GetTaskInfo(database, taskID, StarTime):
    TaskInfo=''
    if not os.path.exists(database):
        conn = CreateConnection(database)
        if conn is not None:
            sql_crate_table = """ CREATE TABLE IF NOT EXISTS TASK_INFO (
                                                ID text NOT NULL UNIQUE,
                                                LAST_UPDATE DATETIME NOT NULL
                                                ); """

            CreateTable(conn, sql_crate_table)
        else:
            print("Error! cannot create the database connection.")
    else:
        conn = CreateConnection(database)

    dataValue = (taskID, StarTime)
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

def CreateConnection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except OSError as e:
        print(e)
    return None

def CreateTable(conn, create_table_sql):
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
    rvalue = ''
    folderName = os.path.basename(path)
    if folderName == '':
        rvalue = 'DISK-' + str(path).split(":")[0]
    else:
        rvalue = folderName
    return rvalue

def MakeFolder(path):
    RCloneRunCommand('mkdir "' + str(path) + '"')

def DeleteFolder(path):
    RCloneRunCommand('purge "' + str(path) + '"')

def GetFolderList(path):
    rvalue=[]
    vjson = json.loads(ListFolderContents(path))
    for element in vjson:
        if element['MimeType'] == 'inode/directory':
            rvalue.append(element['Name'])
    return rvalue

def RCloneRunCommand(command):
    CurrentDir = Path().absolute()
    rcloneCommand = str(Path(CurrentDir / 'rclone.exe ')) + command
    process = subprocess.Popen(rcloneCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    return [out + err]

def RCloneSync(source, destination, backup, logFile, excludedFolders):
    excludeds = excludedFolders.split('|')
    excludeString = ''
    for excluded in excludeds:
        excludeString = excludeString + '--exclude "' + excluded.strip() + '" '

    excludeString = excludeString.strip()
    CurrentDir = Path().absolute()
    params = 'sync ' + AvoidRCloneBug(source) + ' ' + AvoidRCloneBug(destination) + ' --backup-dir ' +  AvoidRCloneBug(backup) + ' -v --log-file ' + logFile + ' ' + excludeString
    rcloneCommand = str(Path(CurrentDir / 'rclone.exe ')) + params
    p = subprocess.Popen( rcloneCommand, shell=True)
    p.wait()
    return p.returncode

def DoesTheFolderExist(localToCheck, folderName):
    folderList = GetFolderList(localToCheck)
    for folder in folderList:
        if str(folderName) == folder:
            return True
    return False

def CheckFileStructDB(path,fileName):
    vjson = json.loads(ListFolderContents(path))
    for element in vjson:
        if (element['MimeType'] == 'application/octet-stream') and (element['Name'] == fileName ):
            return True
    return False

def ClearOldBackups(diffsToRetain, path, folderName):
    rvalue=[]
    vjson = json.loads(ListFolderContents(path))
    for element in vjson:
        if (element['MimeType'] == 'inode/directory') and ( str(element['Name']).startswith(folderName + '-[')):
            rvalue.append(element['Name'])

    if rvalue.__len__() > (diffsToRetain + 1):
        toDelete = (rvalue.__len__() - (diffsToRetain + 1))
        count = 1
        while count <= toDelete:
            teste = (sorted(rvalue)[count - 1])
            RCloneReturn = RCloneRunCommand('purge ' + str(Path(path) / (sorted(rvalue)[count-1])))
            count += 1

def IsTheFolderEmpty(path):
    rvalue = False
    vjson = json.loads(ListFolderContents(path))
    if len(vjson) == 0:
        rvalue = True
    if (len(vjson)==1) and (vjson[0].get('Name')).endswith('.str') :
        rvalue = True
    return rvalue

def SendEmail(email_server,email_port,from_addr,password,to_addr,subject,content):
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject
    body = MIMEText(content, 'html')
    msg.attach(body)

    try:
        server = smtplib.SMTP(email_server, email_port)
        server.login(from_addr, password)
        server.send_message(msg, from_addr=from_addr, to_addrs=[to_addr])
        return("The e-mail was sent sucesfuly.")
    except smtplib.SMTPAuthenticationError:
        return("The username or password is incorrect.")


def WriteLog(path,data):
    with io.open(Path(path), 'a', encoding='utf8') as f:
        f.write(data + '\n')
        f.close()

def SaveFolderStruct(database, directory):
    if not os.path.exists(database):
        conn = CreateConnection(database)
        if conn is not None:
            sql_crate_table = """ CREATE TABLE IF NOT EXISTS STRUCT (
                                                IS_FOLDER boolean NOT NULL,
                                                PATH text NOT NULL UNIQUE
                                                ); """

            CreateTable(conn, sql_crate_table)
        else:
            print("Error! cannot create the database connection.")
    else:
        conn = CreateConnection(database)
    rootFolder = str(directory).split(os.sep)
    rootFolder = rootFolder[rootFolder.__len__() - 1]
    for root, dirs, files in os.walk(directory):
        currFolder = root.split(str(directory))
        dataValue = (os.path.isdir(root), rootFolder + currFolder[1])
        InsertFolderStruct(conn,dataValue)
        for file in files:
            dataValue = (os.path.isdir(Path(Path(root) / file)), os.path.join(rootFolder + currFolder[1], file))
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
    currentDir = os.path.dirname(os.path.realpath(__file__))
    css = '<style>#data{font-family: Verdana, sans-serif;border-collapse: collapse;width: 100%; font-size: 12px;}#data td, #data th {border: 1px solid #000;padding: 8px;}#data tr {background-color: #f1f1f1;}#data th {padding-top: 12px;padding-bottom: 12px;text-align: left;background-color: #2a579a;color: white;}</style>'
    VssTable = ""
    VSSPath = ''
    thtml = ''
    error = 0
    Messages = []
    tableHTML=[]
    Messages.append("Resume of the job:")
    now = datetime.datetime.now()
    startTime= now
    Messages.append("Started on: " + startTime.strftime('%d/%m/%Y %H:%M:%S'))
    today = now.strftime('%Y-%m-%d-%H-%M-%S')
    taskSource = ""
    taskDestination = ""
    configParser = configparser.RawConfigParser()
    configParser.read(taskFile, encoding='utf8')
    taskID = int(configParser.get('TASK', 'id'))
    taskName = str(configParser.get('TASK', 'name'))
    taskFolders = configParser.get('TASK', 'folders')
    taskDiff = int(configParser.get('TASK', 'diff'))
    taskEmailServer = str(configParser.get('TASK', 'email-server'))
    taskEmailPort = str(configParser.get('TASK', 'email-port'))
    taskEmailUser = str(configParser.get('TASK', 'email-user'))
    taskEmailPassword = str(configParser.get('TASK', 'email-password'))
    taskEmailTo = str(configParser.get('TASK', 'email-to'))
    taskExcludedFolders = str(configParser.get('TASK', 'excluded-folders'))

    taskInfoDB = Path(Path().absolute() / 'tasks.sqlite3')
    logName = re.sub('[ \\\\/:"*?<>|]', '', str(taskName + '-' + str(taskID) + '-' + today))
    backupStatus = ''

    #check if log folder exist. if not create folder
    if not DoesTheFolderExist(str(Path(Path().absolute())), 'Logs'):
        MakeFolder(str(Path(Path().absolute() / 'Logs' )))

    #def log file
    logFile= str(Path(Path().absolute() / 'Logs' / str(logName +'.log')))

    LastDataSent = GetTaskInfo(taskInfoDB, taskID, startTime)

    WriteLog(logFile, "Started on: " + startTime.strftime('%d/%m/%Y %H:%M:%S'))

    #Initialize Shadow copy id table
    if str(platform.system()).upper() == "WINDOWS":
        VssTable = GetMappedDrivers()
        if VssTable == "{}":
            VssTable = dict()
        else:
            VssTable = eval(VssTable)

    #check if all source and destination folders exists
    folders = taskFolders.split('|')
    for folder in folders:
        FolderPair = folder.split('->')
        firstTime = 0
        taskSource = str(FolderPair[0]).strip()
        taskDestination = str(FolderPair[1]).strip()

        #create Shadow copy
        if str(platform.system()).upper() == "WINDOWS":
            DiskLetter = GetDriveLetter(str(FolderPair[0]).strip())
            VSSid = VssTable.get(DiskLetter.upper())
            if VSSid is None:
                VssTable[DiskLetter.upper()] = VSSCreate(taskSource)


        #check if source folder exists
        if not os.path.exists(taskSource):
            error=1
            Messages.append('<strong>The source folder "' + taskSource + '" does not exist.</strong>')

        # check if destination  folder exists
        if not DoesTheFolderExist(str(Path(taskDestination).parent), str(ReturnFolderName(taskDestination))):
            error=1
            Messages.append('<strong>The destination folder "' + taskDestination + '" does not exist.</strong>')

    #if no error start send data processs
    if error == 0:
        folders = taskFolders.split('|')
        tableHTML.append('<br><table id="data"><tr><th>Folder</th><th>Status</th></tr>')
        for folder in folders:
            WriteLog(logFile, "=============================================================================")
            WriteLog(logFile, now.strftime('%Y/%m/%d %H:%M:%S') +  " Synchronizing " + str(folder))
            FolderPair = str(folder).split('->')

            taskSource = Path(FolderPair[0].strip())
            taskDestination = Path(FolderPair[1].strip())

            # mount VSS as source if possible
            if str(platform.system()).upper() == "WINDOWS":
                DiskLetter = GetDriveLetter(FolderPair[0].strip())
                VSSid = VssTable.get(DiskLetter.upper())
                if VSSid is not None:
                    VSSPath = VSSMount(VSSid, DiskLetter)
                    taskSource = Path(str(taskSource).replace(VSSPath[0:2],VSSPath))
                else:
                    Messages.append('<strong>Error on mount Volume Shadow Copy.</strong>')


            folderName = ReturnFolderName(taskSource)
            label = datetime.datetime.strptime(LastDataSent[0][1], '%Y-%m-%d %H:%M:%S.%f')
            label = label.strftime('%Y-%m-%d-%H-%M-%S')

            lastFolderStructDBName = folderName + '-' + str(taskID) + '-' + label + '.str'
            folderStructDBName = re.sub('[ \\\\/:"*?<>|]', '', folderName + '-' + str(taskID) + '-' + today + '.str')
            folderStructDB = Path().absolute() / folderStructDBName


            destFull = str(taskDestination / (folderName + '-[FULL]'))
            destDiff = str(taskDestination / (folderName + '-[' + label + ']'))

            #save folder struct to be possible restore files
            SaveFolderStruct(folderStructDB, taskSource)

            #create full folder on dest if not exists
            if not DoesTheFolderExist(str(taskDestination), ReturnFolderName(destFull)):
                firstTime=1
                MakeFolder(destFull)

            # create diff folder on dest if not exists
            if not DoesTheFolderExist(str(taskDestination), ReturnFolderName(destDiff)):
                MakeFolder(destDiff)

            #send data to destination
            RCloneReturn = RCloneSync(str(taskSource), str(destFull), str(destDiff), logFile, taskExcludedFolders)

            #if has no error
            if RCloneReturn == 0:

                tableHTML.append('<tr><td>'+ folderName +'</td><td>')
                #check if have any file modified on diff folder of this folder item
                if IsTheFolderEmpty(str(destDiff)):
                    noChanges = False
                    if firstTime == 1:
                        RCloneRunCommand('copy "' + str(folderStructDB) + '" "' + destFull + '"')
                    else:
                        RCloneRunCommand('move "' + str(Path(destDiff) / lastFolderStructDBName) + '" "' + destFull + '"')
                        noChanges = True


                    #check if folder struct database is on destination full folder
                    if CheckFileStructDB(destFull, folderStructDBName) or noChanges:
                        if firstTime == 1:
                            tableHTML.append('The initial data was sent successfully.</tr>')
                        else:
                            tableHTML.append('No changes on data.</tr>')

                        os.remove(folderStructDB)
                    else:
                        error = 1
                        tableHTML.append('Cannot locate struct folder database on application directory.</tr>')

                    DeleteFolder(destDiff)
                else:
                    RCloneRunCommand('copy "' + str(folderStructDB) + '" "' + destFull + '"')

                    # check if internal  data base file is on destination
                    if CheckFileStructDB(destFull, folderStructDBName):
                        SetTaskInfo(taskInfoDB, taskID, startTime)
                        tableHTML.append('The data was updated successfully.</tr>')
                        os.remove(folderStructDB)
                        # clean old backup folders based on task config
                        ClearOldBackups(taskDiff, str(taskDestination), folderName)
                    else:
                        error = 1
                        tableHTML.append('Cannot locate struct folder database on destination.</tr>')
            else:
                error = 1
                tableHTML.append('<tr><td>' + folderName + '</td><td>')
                tableHTML.append('<strong  style="color: #ff0000;">Finished with some problems. Please check the logs to get a details of the errors.</strong></tr>')

            #dismount VSS
            if VSSPath is not None:
                VSSUnmount(str(Path(VSSPath)))

            # create html  table  tasks result for email body
            for line in tableHTML:
                thtml = thtml + line


    Messages.append(thtml + '</table>')

    #remove created shadow copies
    for value in VssTable.values():
        if value[0] == "{" :
            VSSDelete(value)


    if error == 0:
        backupStatus = 'Status: Success'
        Messages.append('<div style="color: blue"><b>General ' + backupStatus + '</b></div>')
    else:
        backupStatus = 'Status: Failure'
        Messages.append('<div style="color: red"><b>General ' + backupStatus + '</b></div>')

    subject = str(taskName).upper() + "|" + backupStatus
    endTime = datetime.datetime.now()
    elapsedTime = endTime - startTime

    Messages.append("<b>Started on:</b> " + startTime.strftime('%d/%m/%Y %H:%M:%S'))
    Messages.append("<b>Finished on:</b> " + endTime.strftime('%d/%m/%Y %H:%M:%S'))
    Messages.append("<b>Time elapsed:</b> " + str(elapsedTime))

    #prepare email to send
    emailContent = '<html><head>' + css + '</head><body><div style="font-family: Verdana; font-size: 12px;">'
    for line in Messages:
        emailContent = emailContent + line + "<br>"
    emailContent = emailContent + "</div></body></html>"

    #send email
    emailResult = SendEmail(taskEmailServer, taskEmailPort, taskEmailUser, taskEmailPassword, taskEmailTo, subject, emailContent)

    WriteLog(logFile, "=============================================================================")
    WriteLog(logFile, "Email Status: " + emailResult)
    WriteLog(logFile, "Started on: " + startTime.strftime('%d/%m/%Y %H:%M:%S'))
    WriteLog(logFile, "Finished on: " + endTime.strftime('%d/%m/%Y %H:%M:%S'))
    WriteLog(logFile, "Time elapsed: " + str(elapsedTime))

def HasAdministrativePrivilegies():
    rvalue = False
    if str(platform.system()).upper() == "WINDOWS":
        proc = subprocess.Popen(["net", "session"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        if err.decode("utf-8") == "":
            rvalue = True
    else:
        if os.getuid() == 0:
            rvalue = True
    return rvalue


def Main():
    if HasAdministrativePrivilegies():
        currentDir = os.path.dirname(os.path.realpath(__file__))
        os.chdir(currentDir)
        parser = ArgumentParser(description = 'Backup with RClone made by Andrei Bernardo Simoni.')
        parser.add_argument('-t', '--task', action='store', required = True ,dest='taskFile', default='Backup with RClone made by Andrei Bernardo Simoni.', help='Task configuration file to run.')
        arguments = parser.parse_args()
        if os.path.exists(arguments.taskFile):
            RunTask(arguments.taskFile)
        else:
            print('The task file does not exist.')
    else:
        print('This tool need administrative privileges to work correctly.')

Main()