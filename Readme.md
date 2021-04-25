### Python Scripts to backup and restore. 

The scripts are done for python 3 and both require boto3 library.
Both scripts are quite similar and many could be in common libs, just taken apart in files for simplicity.
Backup and recovery scripts has a similar style:

```
  pre_backup_health_check()
  backup()
  post_backup()
```

and the restore like:

```
  pre_restore()
  restore()
  post_restore()
```


On those steps, basic checks are done in AWS and MySQL to understand if the backup and restore can be accomplished.

The scripts are configured in a json file like:

```
{
   "backup":{
      "mysql":{
         "user":"root",
         "pass":"toor",
         "schema":"employees"

      },
      "aws":{
         "access_key":"KeyValue",
         "secret":"Secret",
         "bucket":"bvcket-test"
      }
   },
   "restore":{
      "mysql":{
         "user":"root",
         "pass":"toor",
         "schema":"employees2"
      },
      "aws":{
         "access_key":"KeyValue",
         "secret":"Secret",
         "bucket":"bvcetest",
         "fileschema":"employees.20210424235043.sql.gz",
         "filedata":"employees.20210424235043_data.sql.gz",
         "md5schema":"md5sum_employees.20210424235043",
         "md5data":"md5sum_employees.20210424235043_data"
      }
   }
}
```

Where the file and other details can be setup.

You need to install the boto3 library:

```
$ pip install boto3==1.17.56
```

To run the backup:
```
$ python sql_backup.py -c conf.json -b
```

To run the recover 
```
$ python sql_restore.py -c conf.json -r
```

#### About the scripts:
A lot can be improved and summarized in functions, and standarize the call uses to the OS. The script assume the folder is empty, and can be used. 
Did not test the 150Gb upload to aws, but the transmision settings should be fine for the size mentioned.

#### About the backup itself:
The backup is done gunzipping the output to try to reduce the amount of data, probably look into a different/optimized algorithm for text would be an improvement.

The recovery, the md5sum of that 150 Gb file can take ages, so, it's a poor way, check the file size being the same would be an interesting point, but definitely not enough.
Last, I implemented a simple check tables, to understand if all the tables were there. Likely gather information from the status of the tables and compare the size after the restore is another nice way to look into the health of the DB, those are "cheap queries".


#### Improvements:
Download the file and read from it in a different partition or physical disk than the MySQL is using, that could improve drastically the speed.
Backup the Table information in different files, and parallelize the execution if the I/O can handle it, can drastically decrease the time too.

#### Alternatives:
Probably the fastest way to do a backup is copying the datadir folder of the mysql database if it can be stopped. If you have a mirror mysql instance, I would look into moving one of the DB out of the cluster. Stop it. Copy the data, and re-join to sync the information.

Other than that, I would try to understand if all the information is required to be running in the DB, sometimes there is historical data that can be, at least moved.


