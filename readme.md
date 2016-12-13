# ZFS Snapshot Script

Simple ZFS incremental snaphot to remote host script with prune and job reporting.

## Todo
* ```--only-errors```
    * Only report errors...

## Install
1. Download the script from https://github.com/rennu/zfs-backup
2. Create a private key on backup source host
    ```ssh-keygen -b 4096```
3. Add the public key to Your backup target hosts authorized_keys
4. Allow root to login over ssh only by using the created key
    ```PermitRootLogin prohibit-password```
5. Create crontab job ie. backup filesystem "materials" from pool "tank" to host  "backup-server" every 2nd hour. Remove snapshots after 360/(24/2) = 30 days. Send success/error messages to admin@example.com
    ```0 */2 * * * /usr/bin/python /home/zfsbackup/zfs-backup/zfs-backup.py --pool tank --filesystem materials --snapshots 360 --backuphost root@backup-server --email admin@example.com```

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
    
    --debug
        Output all commands

