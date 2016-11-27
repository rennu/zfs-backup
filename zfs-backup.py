#!/usr/bin/python

import subprocess, re, getopt, sys, json, os, shlex, time, platform


def main():

    options = [
        'pool=',
        'filesystem=',
        'snapshots=',
        'backuphost=',
        'email='
    ]

    optList, bs = getopt.getopt(sys.argv[1:], '', options)
    getOpt = optFinder(optList)

    if getOpt.findKey('--pool') and getOpt.findKey('--backuphost'):
    
        # Set script name to be used later
        scriptName = os.path.basename(sys.argv[0])
        
        # Set hostname for system identification
        hostname = platform.node()
    
        # Get command line arguments
        getOpt.findKey('--pool')
        poolName = getOpt.optValue
        
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
    
        # Base value for pool + filesystem combination
        snapshotBase = poolName
        if filesystemName != "":
            snapshotBase += "/" + filesystemName
    
        # Get local snapshot list

        localSnapshots = getSnapshots(snapshotBase)

        # Search for active processes with similar attributes to prevent running multiple instances
        # of same job
        
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
                            if tPool == poolName and tFilesystem == filesystemName:
                                sendMail(emailAddress, "Backup job cancelled (" + hostname + "): Duplicate job running", "Tried to run backup job but detected duplicate job\nAttempted command:\n" + " ".join(sys.argv))
                                sys.exit(1)                    
                
        # See if pool exists at the destination machine
        
        cmd = ['ssh', backupHost, 'zfs', 'list']
        output = executeCommand(cmd).split("\n")
        poolExists = False
        for outputLine in output:
            rFilesystemName = re.sub(r'\s+', " ", outputLine).split(" ")[0]
            if rFilesystemName == poolName:
                poolExists = True
        
        if poolExists == False:
            sys.exit(1)
            sendEmail(emailAddress, "Backup job failed (" + hostname + "): Target pool does not exist on remote machine")
        
        # List remote snapshots to find snapshot to increment
        remoteSnapshots = getSnapshots(snapshotBase, backupHost)
        
        # Find latest shared snapshot
        fromSnapshot = ""
        if len(remoteSnapshots) > 0:
            fromSnapshot = remoteSnapshots[len(remoteSnapshots)-1]
        
        isIncremental = False
        for snapshot in localSnapshots:
            if snapshot == fromSnapshot:
                isIncremental = True
        
        # Create a new snapshot
        
        snapshotTimestamp = time.strftime("%Y.%m.%d_%H.%M")
        snapshotNameActual = snapshotBase + "@" + snapshotTimestamp
        
        if filesystemName != "":
            cmd = ['zfs', 'snapshot', snapshotNameActual]
        else:
            cmd = ['zfs', 'snapshot', '-r', snapshotNameActual]
            
        output = executeCommand(cmd)


        # Send backup to the backup host
        
        if isIncremental:
            cmd = ['zfs', 'send', '-i', fromSnapshot, snapshotNameActual, '|', 'ssh', backupHost, 'zfs', 'recv', snapshotBase]
        else:
            cmd = ['zfs', 'send', snapshotNameActual, '|', 'ssh', backupHost, 'zfs', 'recv', snapshotBase]

        executeCommandS(cmd) # Notice S for shell
        
        # Update snapshot information
        
        localSnapshots = getSnapshots(snapshotBase)
        remoteSnapshots = getSnapshots(snapshotBase, backupHost)
        
        # Prune local snapshots according to --snapshots argument
        
        if len(localSnapshots) > numSnapshots:
            numDelete = len(localSnapshots) - numSnapshots
            for idx in range(0, numDelete):
                deleteSnapshot = localSnapshots[idx]
                cmd = ['zfs', 'destroy', deleteSnapshot]
                executeCommand(cmd)

        # Prune remote snapshots according to --snapshot argument

        if len(remoteSnapshots) > numSnapshots:
            numDelete = len(remoteSnapshots) - numSnapshots
            for idx in range(0, numDelete):
                deleteSnapshot = remoteSnapshots[idx]
                cmd = ['ssh', backupHost, 'zfs', 'destroy', deleteSnapshot]
                executeCommand(cmd)
        
        # Hopefylly done!
        
    else:
        print """
            Required arguments:
            
            --pool [string]
                Pool to backup ie. tank
                
            --backuphost [username@hostname]
                Also, you need to be able to login using ssh private key

            --filesystem [string] (optional)
                Filesystem to backup ie. midgets
            
            --snapshots [int] (optional)
                Number of snapshots to keep
                Default: 10
                
            --email user@host (optional)
                Send job related messages as email
                
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

def sendMail(emailAddress, subject, body):
    print """
        To: %s
        Subject: %s
        Body: %s
        """ % (emailAddress, subject, body)

def executeCommand(cmd):

    print ' '.join(cmd)

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, errors = p.communicate()

    if p.returncode != 0:
        print errors
        sys.exit(1)
    else:
        return output


def executeCommandS(cmd):
    
    cmd = ' '.join(cmd)
    print cmd
    
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, errors = p.communicate()

    if p.returncode != 0:
        print errors
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