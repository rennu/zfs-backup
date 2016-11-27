#!/usr/bin/python

import subprocess, re, getopt, sys, json, os, time, platform

debug = False
emailAddress = ""
hostname = platform.node()

def main():

    global debug
    global emailAddress

    options = [
        'pool=',
        'targetpool=',
        'filesystem=',
        'snapshots=',
        'backuphost=',
        'email=',
        'debug'
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
        
        getOpt.findKey('--snapshots')
        numSnapshots = getOpt.optValue
        if numSnapshots != "":
            try:
                numSnapshots = int(numSnapshots)
            except ValueError:
                print "Error: Invalid --snapshots value"
                sys.exit()
        else:
            numSnapshots = 5
        
        getOpt.findKey('--backuphost')
        backupHost = getOpt.optValue
        
        getOpt.findKey('--email')
        emailAddress = getOpt.optValue

        if getOpt.findKey('--debug'):
            debug = True
    
        # Base value for pool + filesystem combination
        localSnapshotBase = localPoolName
        remoteSnapshotBase = targetPoolName
        if filesystemName != "":
            localSnapshotBase += "/" + filesystemName
            remoteSnapshotBase += "/" + filesystemName
    
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
                        if procfile[0] == "python" and os.path.basename(procfile[1]) == scriptName:
                    
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
        cmd = ['ssh', backupHost, 'zfs', 'list']
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
            cmd = ['zfs', 'snapshot', snapshotNameActual]
        else:
            cmd = ['zfs', 'snapshot', '-r', snapshotNameActual]
            
        output = executeCommand(cmd)

        # Send backup to the backup host
        if isIncremental:
            cmd = ['zfs', 'send', '-i', fromSnapshot, snapshotNameActual, '|', 'ssh', backupHost, 'zfs', 'recv', remoteSnapshotBase]
        else:
            cmd = ['zfs', 'send', snapshotNameActual, '|', 'ssh', backupHost, 'zfs', 'recv', remoteSnapshotBase]

        executeCommand(cmd, True) # Notice S for shell
        
        # Update snapshot information
        localSnapshots = getSnapshots(localSnapshotBase)
        remoteSnapshots = getSnapshots(remoteSnapshotBase, backupHost)
        
        # Prune local snapshots according to --snapshots argument
        if len(localSnapshots) > numSnapshots:
            numDelete = len(localSnapshots) - numSnapshots
            for idx in range(0, numDelete):
                destroySnapshot = localSnapshots[idx]
                cmd = ['zfs', 'destroy', destroySnapshot]
                executeCommand(cmd)

        # Prune remote snapshots according to --snapshot argument
        if len(remoteSnapshots) > numSnapshots:
            numDelete = len(remoteSnapshots) - numSnapshots
            for idx in range(0, numDelete):
                destroySnapshot = remoteSnapshots[idx]
                cmd = ['ssh', backupHost, 'zfs', 'destroy', destroySnapshot]
                executeCommand(cmd)
        
        # Hopefylly done!
        
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
                
            --email user@host (optional)
                Send job related messages as email
            
            --debug
                Output all commands
            
        """

def getSnapshots(snapshotBase, backupHost=""):

    if backupHost != "":
        cmd = ['ssh', backupHost, 'zfs', 'list', '-t', 'snapshot']
    else:
        cmd = ['zfs', 'list', '-t', 'snapshot']    
        
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

def logError(title, body):
    sendMail(emailAddress, title, body)
    print title
    print body

def sendMail(emailAddress, subject, body):
    print """
        To: %s
        Subject: %s
        Body: %s
        """ % (emailAddress, subject, body)

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