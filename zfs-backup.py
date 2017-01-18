#!/usr/bin/python

zfsBin = "/sbin/zfs"
sshBin = "/usr/bin/ssh"

import subprocess, re, getopt, sys, json, os, time, platform, smtplib, math
from email.mime.text import MIMEText

debug = False
emailAddress = ""
hostname = platform.node()
smtpServer = "localhost"
sender = "zfsbackup@" + hostname
numSnapshots = 10
onlyErrors = False

def main():

    global debug
    global emailAddress
    global smtpServer
    global sender
    global numSnapshots
    global onlyErrors

    jobStartTime = int(time.time())

    options = [
        'pool=',
        'targetpool=',
        'filesystem=',
        'snapshots=',
        'backuphost=',
        'email=',
        'smtp=',
        'sender=',
        'debug',
        'only-errors'
    ]

    optList, bs = getopt.getopt(sys.argv[1:], '', options)
    getOpt = optFinder(optList)

    if getOpt.findKey('--pool') and getOpt.findKey('--backuphost'):
    
        # Set script name to be used later
        scriptName = os.path.basename(sys.argv[0])
        
        # Get command line arguments
        getOpt.findKey('--pool')
        localPoolName = getOpt.optValue

        targetPoolName = localPoolName
        if getOpt.findKey('--targetpool'):
            targetPoolName = getOpt.optValue
        
        getOpt.findKey('--filesystem')
        filesystemName = getOpt.optValue
        
        if getOpt.findKey('--snapshots'):
            numSnapshots = getOpt.optValue
            if numSnapshots != "":
                try:
                    numSnapshots = int(numSnapshots)
                except ValueError:
                    print "Error: Invalid --snapshots value"
                    sys.exit()
            else:
                numSnapshots = 10
        
        getOpt.findKey('--backuphost')
        backupHost = getOpt.optValue
        
        getOpt.findKey('--email')
        emailAddress = getOpt.optValue

        if getOpt.findKey('--smtp'):
            smtpServer = getOpt.optValue

        if getOpt.findKey('--sender'):
            sender = getOpt.optValue

        if getOpt.findKey('--debug'):
            debug = True
        
        if getOpt.findKey('--only-errors'):
            onlyErrors = True
    
        # Base value for pool + filesystem combination
        localSnapshotBase = localPoolName
        remoteSnapshotBase = targetPoolName
        if filesystemName != "":
            localSnapshotBase += "/" + filesystemName
            remoteSnapshotBase += "/" + filesystemName

        # Does localSnapshotBase actually exist?
        output = executeCommand([zfsBin, 'list']).split("\n")
        localSnapshotBaseExists = False
        for filesystem in output:
            filesystem = re.sub("\s+", " ", filesystem).split(" ")[0]
            matchString = r'^' + localSnapshotBase + '$'
            if re.match(matchString, filesystem):
                localSnapshotBaseExists = True
    
        if localSnapshotBaseExists == False:
            logError("Backup job failed (" + hostname + ")", "Local snapshot filesystem / pool (" + localSnapshotBase + ") does not exist")
            sys.exit()
        
        # Get local snapshot list
        localSnapshots = getSnapshots(localSnapshotBase)

        # Search for active processes with similar attributes to prevent running multiple instances
        # of same job (ie. when running script automatically by crontab)
        psList = executeCommand(['ps', 'xauww']).split("\n")
        myPid = str(os.getpid())
        
        for ps in psList:
            if re.search(scriptName, ps):
                pid = re.sub(r'\s+', " ", ps).split(" ")[1]
                if pid != myPid:
                    fp = open("/proc/" + pid + "/cmdline", "r")
                    procfile = fp.read().split("\x00")
                    fp.close()
            
                    if len(procfile) > 2:
                        if os.path.basename(procfile[0]) == "python" and os.path.basename(procfile[1]) == scriptName:
                    
                            optList2, bs = getopt.getopt(sys.argv[1:], '', options)
                            getOpt2 = optFinder(optList2)
                        
                            getOpt2.findKey("--pool")
                            tPool = getOpt2.optValue
                            getOpt2.findKey("--filesystem")
                            tFilesystem = getOpt2.optValue
                            if tPool == localPoolName and tFilesystem == filesystemName:
                                logError("Backup job cancelled (" + hostname + "): Duplicate job running", "Tried to run backup job but detected duplicate job\nAttempted command:\n" + " ".join(sys.argv))
                                sys.exit(1)                    
                
        # See if pool exists at the destination machine        
        cmd = [sshBin, "-o", 'StrictHostKeyChecking no', backupHost, 'zfs', 'list']
        output = executeCommand(cmd).split("\n")
        poolExists = False
        for outputLine in output:
            rPoolName = re.sub(r'\s+', " ", outputLine).split(" ")[0]
            if rPoolName == targetPoolName:
                poolExists = True
        
        if poolExists == False:
            logError("Backup job failed (" + hostname + "): Target pool (" + targetPoolName + ") does not exist on remote machine", "Could not find target pool " + targetPoolName + " on backup target " + backupHost)
            sys.exit(1)
            
        # List remote snapshots to find snapshot to increment
        remoteSnapshots = getSnapshots(remoteSnapshotBase, backupHost)
        
        # Find latest shared snapshot (zfs list -t snapshots list snapshots in ascending time order)
        fromSnapshot = ""
        if len(remoteSnapshots) > 0:
            fromSnapshot = remoteSnapshots[len(remoteSnapshots)-1]
        
        isIncremental = False
        for snapshot in localSnapshots:
            if snapshot == fromSnapshot:
                isIncremental = True
        
        # Create a new snapshot
        snapshotTimestamp = time.strftime("%Y.%m.%d_%H.%M")
        snapshotNameActual = localSnapshotBase + "@" + snapshotTimestamp
        
        if filesystemName != "":
            cmd = [zfsBin, 'snapshot', snapshotNameActual]
        else:
            cmd = [zfsBin, 'snapshot', '-r', snapshotNameActual]
            
        output = executeCommand(cmd)

        # Send backup to the backup host
        if isIncremental:
            cmd = [zfsBin, 'send', '-i', fromSnapshot, snapshotNameActual, '|', sshBin, "-o", '"StrictHostKeyChecking no"', backupHost, 'zfs', 'recv', remoteSnapshotBase]
        else:
            cmd = [zfsBin, 'send', snapshotNameActual, '|', sshBin, "-o", '"StrictHostKeyChecking no"', backupHost, 'zfs', 'recv', remoteSnapshotBase]

        executeCommand(cmd, True) # Notice S for shell
        
        # Update snapshot information
        localSnapshots = getSnapshots(localSnapshotBase)
        remoteSnapshots = getSnapshots(remoteSnapshotBase, backupHost)
        
        # Prune local snapshots according to --snapshots argument
        if len(localSnapshots) > numSnapshots:
            numDelete = len(localSnapshots) - numSnapshots
            for idx in range(0, numDelete):
                destroySnapshot = localSnapshots[idx]
                # Double check snapshot name
                if not re.search(r'@', destroySnapshot):
                    logError("Backup Job Failed (" + hostname + "): Tried to destroy snapshot with incorrect name: " + destroySnapshot)
                    sys.exit(1)
                else:
                    cmd = [zfsBin, 'destroy', destroySnapshot]
                    executeCommand(cmd)

        # Prune remote snapshots according to --snapshot argument
        if len(remoteSnapshots) > numSnapshots:
            numDelete = len(remoteSnapshots) - numSnapshots
            for idx in range(0, numDelete):
                destroySnapshot = remoteSnapshots[idx]
                # Double check snapshot name
                if not re.search(r'@', destroySnapshot):
                    logError("Backup Job Failed (" + hostname + "): Tried to destroy snapshot with incorrect name: " + destroySnapshot)
                    sys.exit(1)
                else:
                    cmd = [sshBin, "-o", 'StrictHostKeyChecking no', backupHost, 'zfs', 'destroy', destroySnapshot]
                    executeCommand(cmd)
        
        # Hopefylly done!
        jobEndTime = int(time.time())
        timeDifference = jobEndTime - jobStartTime
        hours = math.floor(timeDifference / 60 / 60)
        minutes = math.floor((timeDifference - hours * 60 * 60) / 60)
        seconds = math.floor(timeDifference - (hours * 60 * 60 + minutes * 60))

        timeUsed = str(int(hours)) + "h " + str(int(minutes)) + "m " + str(int(seconds)) + "s"

        logError("Backup job completed successfully (" + hostname + ")", "Completed backing up on " + hostname + "\n\nBackup filesystem: " + localSnapshotBase + "\nSnapshot name: " + snapshotNameActual + "\nTask completion time: " + timeUsed, True)
        
    else:
        print """
            Required arguments:
            
            --pool [string]
                Pool to backup ie. tank
            
            --targetpool [string] (optional)
                On target host use different pool name
                
            --backuphost [username@hostname]
                Also, you need to be able to login using ssh private key

            --filesystem [string] (optional)
                Filesystem to backup ie. midgets
            
            --snapshots [int] (optional)
                Number of snapshots to keep
                Default: 10
                
            --email user@host;user2@host2 (optional)
                Send job related messages as email

            --sender foo@bar
                Email sender address
                Default: zfsbackup@$hostname
                
            --smtp hostname (optional)
                SMTP server address
                Default: localhost

            --only-errors
                Only send a job report if job has failed
            
            --debug
                Output all commands
            
        """

# Get list of snapshots as a list
def getSnapshots(snapshotBase, backupHost=""):

    # snapshot]# zfs list -H -t snapshot -o name -s creation

    if backupHost != "":
        cmd = [sshBin, backupHost, zfsBin, 'list', '-H', '-t', 'snapshot', '-o', 'name', '-s', 'creation']
    else:
        cmd = [zfsBin, 'list', '-H', '-t', 'snapshot', '-o', 'name', '-s', 'creation']
        
    output = executeCommand(cmd)

    output = output.split("\n")

    # Parse output to list
    snapshots = []
    snapshotSearchString = r'^' + snapshotBase + '@'
    for outputLine in output:
        if re.match(snapshotSearchString, outputLine):
            snapshotName = outputLine.split(" ")[0]
            snapshots.append(snapshotName)

    return snapshots

# Log errors, duh?
def logError(title, body, isSuccess = False):
    
    if emailAddress != "":
        if (onlyErrors == True and isSuccess == False) or onlyErrors == False:
            sendMail(emailAddress, title, body)
    else:
        print title
        print body

# ...
def sendMail(emailAddress, subject, body):

    if debug:
        print """
To: %s
Subject: %s
Body: %s
""" % (emailAddress, subject, body)

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = emailAddress
    
    smtp = smtplib.SMTP(smtpServer)
    smtp.sendmail(sender, emailAddress.split(";"), msg.as_string())
    smtp.quit()



# Set shell = True when Popen shell=True is required
def executeCommand(cmd, shell = False):

    cmdJoined = ' '.join(cmd)

    if debug == True:
        print cmdJoined

    try:
        if shell == False:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(cmdJoined, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    except OSError as e:
        logError("Backup failed (" + hostname + ")", "Could not execute command:\"" + cmdJoined + "\n" + str(e))
        sys.exit(1)
        
    output, errors = p.communicate()

    if p.returncode != 0:
        logError("Backup failed (" + hostname + ")", "Could not execute command:\n" + cmdJoined + "\n\nOutput:\n" + errors)
        sys.exit(1)
    else:
        return output

# Fugly command line option finder
class optFinder:
    optList = []
    optValue = ""
    
    def __init__(self, *optList):
        self.optList = optList
    
    def findKey(self, optKey):
        self.optValue = ""
        
        for oKey, oValue in self.optList[0]:
            if oKey == optKey:
                self.optValue = oValue
                return True
                
        return False
        
main()