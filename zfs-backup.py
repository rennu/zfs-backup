#!/usr/bin/python

import subprocess, re, argparse, sys, json, os, time, platform, smtplib, math
from email.mime.text import MIMEText

# Configurables
zfsBin = "/sbin/zfs"
sshBin = "/usr/bin/ssh"

# Defaults
debug = False
emailAddress = ""
hostname = platform.node()
smtpServer = "localhost"
sender = "zfsbackup@" + hostname
numSnapshots = 10
onlyErrors = False
cipher = ""

# Globals
sshCmdBase = ['-o', 'StrictHostKeyChecking=no']
cmdLog = []

def main():

    global debug
    global emailAddress
    global smtpServer
    global sender
    global numSnapshots
    global onlyErrors
    global cipher
    global sshCmdBase

    jobStartTime = int(time.time())

    args, scriptName = parseArgs()

    # Set values from command line arguments

    localPoolName = args.pool

    targetPoolName = localPoolName
    if args.targetpool != "":
        targetPoolName = args.targetpool

    backupHost = args.backuphost
    
    filesystemName = args.filesystem

    numSnapshots = args.snapshots
    emailAddress = args.email
    smtpServer = args.smtp
    sender = args.sender
    debug = args.debug
    onlyErrors = args.only_errors
    cipher = args.cipher

    if localPoolName != "" and backupHost != "":


        if cipher != "":
            systemCiphers = executeCommand([sshBin, '-Q', 'cipher']).split("\n")
            if cipher in systemCiphers:
                sshCmdBase = [sshBin, '-c', cipher] + sshCmdBase
            else:
                logError("Backup job failed ({0})".format(hostname), "User defined cipher \"{0}\" is not available on the system.".format(cipher))
                sys.exit()
        else:
            sshCmdBase = [sshBin] + sshCmdBase


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
            if re.search(matchString, filesystem):
                localSnapshotBaseExists = True
    
        if localSnapshotBaseExists == False:
            logError("Backup job failed ({0})".format(hostname), "Local snapshot filesystem / pool ({0}) does not exist".format(localSnapshotBase))
            sys.exit()
        
        # Get local snapshot list
        localSnapshots = getSnapshots(localSnapshotBase)

        # Search for active processes with similar attributes to prevent running multiple instances
        # of same job (ie. when running script automatically by crontab and one round takes more than 24 hours)
        psList = executeCommand(['ps', 'xauww']).split("\n")

        myPid = str(os.getpid())
        ppid = str(os.getppid())

        wr = open("/tmp/ps", "w")
        for i in psList:
            wr.write(i + "\n")
        wr.close()

        for ps in psList:
            if re.search(scriptName, ps):
                pid = re.sub(r'\s+', " ", ps).split(" ")[1]

                # On some systems (probably due to crontab) ps lists two entries for the script (child and parent).
                # Therefore we compare pids and ppids
                if pid != myPid and pid != ppid:

                    fp = open("/proc/" + pid + "/cmdline", "r")
                    procfile = fp.read().split("\x00")
                    fp.close()

                    if len(procfile) > 2:

                        sliceIdx = 1
                        for i in procfile:
                            if re.search(r'' + scriptName + '$', i):
                                break
                            sliceIdx += 1

                        procArgs, scriptName2 = parseArgs(procfile[sliceIdx:-1])

                        if procArgs.pool == localPoolName and procArgs.filesystem == filesystemName:
                            logError("Backup job cancelled ({0}): Duplicate job running".format(hostname), "Tried to run backup job but detected duplicate job\nAttempted command:\n{0}".format(" ".join(sys.argv)))
                            sys.exit(1)                    

        # See if pool exists at the destination machine        
        cmd = sshCmdBase + [backupHost, 'zfs', 'list']
        output = executeCommand(cmd).split("\n")
        poolExists = False
        for outputLine in output:
            rPoolName = re.sub(r'\s+', " ", outputLine).split(" ")[0]
            if rPoolName == targetPoolName:
                poolExists = True
                break
        
        if poolExists == False:
            logError("Backup job failed ({0}): Target pool ({1}) does not exist on remote machine".format(hostname, targetPoolName), "Could not find target pool {0} on backup target {1}".format(targetPoolName, backupHost))
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
                break
        
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
            cmd = [zfsBin, 'send', '-i', fromSnapshot, snapshotNameActual, '|']
        else:
            cmd = [zfsBin, 'send', snapshotNameActual, '|']

        cmd += sshCmdBase + [backupHost, 'zfs', 'recv', remoteSnapshotBase]


        executeCommand(cmd, True) # Need shell=True because of pipe
        
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
                    logError("Backup Job Failed ({0}): Tried to destroy snapshot with incorrect name: {1}" .format(hostname, destroySnapshot))
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
                    logError("Backup Job Failed ({0}): Tried to destroy snapshot with incorrect name: {1}".format(hostname, destroySnapshot))
                    sys.exit(1)
                else:
                    cmd = sshCmdBase + [backupHost, 'zfs', 'destroy', destroySnapshot]
                    executeCommand(cmd)
        
        # Hopefylly done!
        jobEndTime = int(time.time())
        timeDifference = jobEndTime - jobStartTime
        hours = math.floor(timeDifference / 60 / 60)
        minutes = math.floor((timeDifference - hours * 60 * 60) / 60)
        seconds = math.floor(timeDifference - (hours * 60 * 60 + minutes * 60))

        timeUsed = str(int(hours)) + "h " + str(int(minutes)) + "m " + str(int(seconds)) + "s"

        logError("Backup job completed successfully ({0})".format(hostname), "Completed backing up on {0}\n\nBackup filesystem: {1}\nSnapshot name: {2}\nTask completion time: {3}".format(hostname, localSnapshotBase, snapshotNameActual, timeUsed), True)



def parseArgs(parseList = []):

    # Create command line arguments parser
    parser = argparse.ArgumentParser()

    # Mandatory
    parser.add_argument("--pool", required = True, 
        help = "Name of the pool to be backed up")
    parser.add_argument("--backuphost", required = True,
        help = "Hostname or IP of the backup host")

    # Optional
    parser.add_argument("--targetpool", default = "",
        help = "Name of the target pool on remote host")
    parser.add_argument("--filesystem", default = "",
        help = "Name of the filesystem to be backed up")
    parser.add_argument("--snapshots", default = numSnapshots, type = int,
        help = "Number of snapshots to keep before prune")
    parser.add_argument("--email", nargs = "*", default = "",
        help = "Receive error messages by email.")
    parser.add_argument("--sender", default = sender,
        help = "Set sender email for email messages")
    parser.add_argument("--smtp", default = "localhost",
        help = "SMTP server hostname")
    parser.add_argument("--only-errors", action="store_true",
        help = "Only report errors")
    parser.add_argument("--cipher", default = "", 
        help = "Use ssh cipher other than system default")
    parser.add_argument("--debug", action="store_true",
        help = "Print commands while executing")

    if len(parseList) > 0:
        return parser.parse_args(parseList), parser.prog
    else:
        return parser.parse_args(), parser.prog
    

# Get list of snapshots as a list
def getSnapshots(snapshotBase, backupHost=""):

    # snapshot]# zfs list -H -t snapshot -o name -s creation

    if backupHost != "":
        cmd = sshCmdBase + [backupHost, zfsBin, 'list', '-H', '-t', 'snapshot', '-o', 'name', '-s', 'creation']
    else:
        cmd = [zfsBin, 'list', '-H', '-t', 'snapshot', '-o', 'name', '-s', 'creation']
        
    output = executeCommand(cmd)

    output = output.split("\n")

    # Parse output to list
    snapshots = []
    snapshotSearchString = r'^' + snapshotBase + '@'
    for outputLine in output:
        if re.search(snapshotSearchString, outputLine):
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
To: {0}
Subject: {1}
Body: {2}
Commands:
# {3}
""".format(emailAddress, subject, body, '\n# '.join(cmdLog))

    body += """
Commands:
# {0}
""".format('\n# '.join(cmdLog))

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ';'.join(emailAddress)
    
    smtp = smtplib.SMTP(smtpServer)
    smtp.sendmail(sender, emailAddress, msg.as_string())
    smtp.quit()

# Set shell = True when Popen shell=True is required
# Needed for |
def executeCommand(cmd, shell = False):

    global cmdLog

    cmdJoined = ' '.join(cmd)
    cmdLog.append(cmdJoined)

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


main()