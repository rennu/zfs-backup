# ZFS Backup Script

Simple ZFS incremental backup to remote host script with prune and job reporting.

## Installation
1. Download the script from https://github.com/rennu/zfs-backup
2. Create a private key on backup source host

    ```ssh-keygen -b 4096```
3. Add the public key to Your backup target host's authorized_keys
4. Create backup target pool on backup target host
5. Allow root to login over ssh only by using the created key

    ```PermitRootLogin prohibit-password```
6. Create a crontab job, set the backup script to be run every 2nd hour

    ```0 */2 * * * /usr/bin/python /opt/zfs-backup/zfs-backup.py --pool tank --filesystem materials --snapshots 360 --backuphost root@backup-server --email admin@example.com```

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
    
## TODO

* Backup to local media ie. USB drive
* 