#!/usr/bin/python

import subprocess, re, getopt, sys


def main():

    options = [
        'pool=',
        'filesystem=',
        'snapshots=',
        'backuphost='
    ]

    optList, bs = getopt.getopt(sys.argv[1:], '', options)
    getOpt = optFinder(optList)

    if getOpt.findKey('--pool') and getOpt.findKey('--backuphost'):
    
        # Get command line arguments
        getOpt.findKey('--pool')
        poolName = getOpt.optValue
        
        getOpt.findKey('--filesystem')
        filesystemName = getOpt.optValue
        
        getOpt.findKey('--snapshots')
        snapshotsNumber = getOpt.optValue
        
        getOpt.findKey('--backuphost')
        backupHost = getOpt.optValue
    
        # Base value for pool + filesystem combination
        snapshotBase = poolName
        if filesystemName != "":
            snapshotBase += "/" + filesystemName
    
        # Get local snapshot list
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

        # Create lock file to prevent multiple instances of same backup job
        
        # Create a new snapshot
        
        # Send backup to the backup host
        
        # Prune local snapshots according to --snapshots argument
        
        # Get remote snapshots list
        
        # Prune remote snapshots according to --snapshots argument
        
        # Remove lock file
        
        
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
                
        """

def executeCommand(cmd):
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, errors = p.communicate()

        if p.returncode != 0:
            print "Error while executing zfs list -t snaphosts\n"
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