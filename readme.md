# ZFS Backup Script

Simple ZFS incremental backup to remote host script with retention and job reporting.

## Installation

We use ```root``` user in this simple example. It is advisable to create a separate account with permission to use zfs tools to run this utility.

##### 01. Setup access from backup source to backup target:
1. Create a new private key on backup source host:
    ```ssh-keygen -b 4096```
2. Add source public key to target authorized_hosts.
3. Make sure that the user is able to connect target from source without entering password. In some systems you need to add ```PermitRootLogin without-password``` in sshd settings.

##### 02. Install the utility
1. Clone the script repository from https://github.com/rennu/zfs-backup. In this example the repository was cloned to ```/opt/zfs-backup```
2. Create a crontab job. In this example we backup filesystem tank/materials every 2nd hour to backup-host, keep 360 latest snapshots (30 days) and send a backup report to admin@example.com
```0 */2 * * * /usr/bin/python /opt/zfs-backup/zfs-backup.py --pool tank --filesystem materials --snapshots 360 --backuphost root@backup-server --email admin@example.com```

    Here are the used arguments listed:
    
    **--pool tank**
    Name of the pool to be backed up (tank)

    **--filesystem materials**
    Name of the filesystem to be backed up (materials)

    **--snapshots 360**
    Keep 360 snapshots and automatically delete older ones

    **--backuphost root@backup-server**
    Username and host of the backup target host

    **--email admin@example.com**
    Address where job reports are sent

## Arguments
    --pool [string]
        Pool to backup ie. tank
    
    --targetpool [string] (optional)
        Pool name on the backup target host. If not set, --pool value will be used.
    
    --backuphost [username@hostname]
        Backup target username and hostname
    
    --filesystem [string] (optional)
        Filesystem to backup eg. materials. If not set, the whole --pool will be backed up
    
    --snapshots [int] (optional)
        Number of snapshots to keep
        Default: 10
    
    --email user@host;user2@host2 (optional)
        Send backup job reports to these email addresses. Separated by semicolon
    
    --sender foo@bar
        Email address to use as sender address
        Default: zfsbackup@hostname
    
    --smtp hostname (optional)
        SMTP server address
        Default: localhost
    
    --only-errors
        Only send a job report if job has failed

    --cipher
        Use ssh cipher other than system default. Some ciphers are fasther than others while some are less secure. You can list available ciphers by using ```ssh -Q cipher```. In the tests aes256-gcm@openssh.com was found to be performant.

    --debug
        Output all commands
    
## TODO

* Backup to local media ie. USB drive.
* Execute the script from backup target ie. backup systems that are outside company firewall.