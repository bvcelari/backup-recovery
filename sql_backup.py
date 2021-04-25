# !/usr/bin/python3

import argparse
import textwrap
import logging
import json
import os
import subprocess
from subprocess import Popen, PIPE
from datetime import datetime

import boto3
from boto3.session import Session
import smtplib, ssl

logger = logging.getLogger('backup-logger')
logger.setLevel(logging.INFO)
# I am assuming can create the file and there are no issues
fileHandler = logging.FileHandler('restore.log')
# you can define log levels for file output and Console output
fileHandler.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)
# logger.info('information message')

parser = argparse.ArgumentParser(description='Backup and Restore script', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-l', '--log-level', default='INFO', help='set log level')
# Instead of tedious parameters that can end up messing, I found a file to define all the possible settings simplier and easier to extent
parser.add_argument('-c', '--config-file', default='', required=True, help=textwrap.dedent('''
Json Config file: 
{
   "backup":{
      "mysql":{
         "user":"user",
         "pass":"pass",
         "schema":"schema"
      },
      "aws":{
         "access_key":"KeyValue",
         "secret":"SecretValu",
         "bucket":"BucketName"
      }
   },
   "restore":{
      "mysql":{
         "user":"user",
         "pass":"pass",
         "schema":"schema"
      },
      "aws":{
         "access_key":"KeyValue",
         "secret":"SecretValu",
         "bucket":"BucketName"
         "filename":"FileName"
      }

   }
}
''')
)
# As arguments, backup and restore are Mutual eXclusive Group one and the other
mxg = parser.add_mutually_exclusive_group(required=True)
mxg.add_argument('-b', '--backup' , default=False, action="store_true", help='Start backup')

args = parser.parse_args()

def send_email(msg):
  port = 465  #  For SSL
  smtp_server = "smtp.gmail.com"
  sender_email = "bvcelari@gmail.com"  #  Enter your address
  receiver_email = "bvcelari@gmail.com"  #  Enter receiver address
  # You need to enable it in google https://www.google.com/settings/security/lesssecureapps
  password = "Your password here"
  message = '''
Subject: Backup Notification 
''' + msg
  context = ssl.create_default_context()
  with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
      server.login(sender_email, password)
      server.sendmail(sender_email, receiver_email, message)


def parse_config(config_file):
  try:
    with open(config_file, 'r') as j:
      json_data = json.load(j)
  except:
    msg = "error in the json file"
    logger.error(msg, exc_info)
    send_email(msg)
    raise
  return json_data

def pre_backup_health_check():
  logger.info("Pre Backup Health Check Starting... ")
  # You need to check user, pass and schema to mysql, I am assuming here that mysql cmd exist
  cmd = 'mysql -u {0}  -p{1} -e \'use {2}  ;\''.format(backup_mysql_user, backup_mysql_pass, backup_mysql_schema) 
  check_mysql = subprocess.call(cmd, shell=True)
  logger.info("Checking Mysql... ")
  if check_mysql != 0:
    msg="error checking the DB with the command: {0}".format(cmd)
    logger.error(msg)
    send_email(msg)
    exit(1)
  logger.info("Mysql Ok")
  # You need to check s3 endpoint exist and you have credentials to write there. You can use the cli or the boto library, I stick to the boto library  
  logger.info("Checking AWS S3... ")
  client = boto3.client(
    's3',
    aws_access_key_id=backup_aws_s3key,
    aws_secret_access_key=backup_aws_secret,
  )
  my_buckets = client.list_buckets()
  for bucket in my_buckets['Buckets']:
    if backup_aws_bucket == bucket['Name']:
      logger.info("We have the bucket: {0}".format(backup_aws_bucket))
      break
  else:
    msg = "Cannot find the bucket: {0}".format(backup_aws_bucket)
    logger.error(msg)
    send_email(msg)
    exit(1)
  logger.info("AWS Ok")
  # You need to check the free space locally, you would need 150 Gb + Estimation of the compressed file !
  # not sure if will have time to properly parse it, but:
  # retrieve current partition would be: df -P . | sed -n '$s/[^%]*%[[:blank:]]*//p'  
  # retreive free space would be df -h  $(df -P . | sed -n '$s/[^%]*%[[:blank:]]*//p')
  # parsing it like: df -h  $(df -P . | sed -n '$s/[^%]*%[[:blank:]]*//p') |awk '{print $4}' | sed 's/G//'|tail -n 1
  # should give you athe ammount of Gigas free where we are goint to store temporary the file
  # not sure if there is way to upload while you gather the mysql output, so you do not need to store it locally, bests would be to store it compressed like $ mysqldump -u [uname] -p[pass] schema| gzip -9 > schema.sql.gz  
  logger.info("Pre Backup Health Check Finished")
  return True 

def backup():
  logger.info("Backup Starting... ")
  # library OS is better for the scenario
  # wuold be nice to check iuf the destination file exist, permissions to write... etc
  try:
    logger.info("Creating schema...")
    mysql_import = os.system("mysqldump -u "+backup_mysql_user+" -p"+backup_mysql_pass+" --no-data "+backup_mysql_schema+"| gzip -9 > "+backup_mysql_filename_schema)
    # we generate a checksum file
    # likely there is something faster for data integrity, need to research this
    md5_generation = os.system("md5sum "+backup_mysql_filename_schema+" > "+backup_mysql_md5sum_schema)
    logger.info("Finished Schema")
    logger.info("Gathering DB data...")
    mysql_import = os.system("mysqldump -u "+backup_mysql_user+" -p"+backup_mysql_pass+"  "+backup_mysql_schema+"| gzip -9 > "+backup_mysql_filename_data+"")
    # we generate a checksum file 
    md5_generation = os.system("md5sum "+backup_mysql_filename_data+" > "+backup_mysql_md5sum_data)
    logger.info("Finished gathering DB data")
  except:
    msg = "An error ocurred while executing the backup"
    logger.error(msg, exc_info=True)
    send_email(msg)
    raise
  logger.info("Backup Finished")
  return True


def post_backup():
  logger.info("Post Backup Starting...")
  # upload files
  try:
    # I could reuse the previous client, but would require some extra refactor that will be paid in readability
    client = boto3.client(
      's3',
      aws_access_key_id=backup_aws_s3key,
      aws_secret_access_key=backup_aws_secret,
    )
    # Maximun size you can upload to S3 is 5 Tb, Boto, has different ways to upload, one is as a MultiPart, that seems convinient so you can resume multiparts if somethins goes wrong, but I am going to stick the simplest scenario:
    tc = boto3.s3.transfer.TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                        multipart_chunksize=1024*25, use_threads=True)
    # you can implement somthig like ProgressPercentage  to show %
    t = boto3.s3.transfer.S3Transfer(client=client, config=tc)
    t.upload_file(backup_mysql_filename_schema, backup_aws_bucket, backup_mysql_filename_schema) 
    t.upload_file(backup_mysql_filename_data, backup_aws_bucket, backup_mysql_filename_data) 
    # I will keep an md5sum of the file, for integrity
    t.upload_file(backup_mysql_md5sum_data, backup_aws_bucket, backup_mysql_md5sum_data)
    t.upload_file(backup_mysql_md5sum_schema, backup_aws_bucket, backup_mysql_md5sum_schema)
    # check files and sizes if possible in AWS
    # Create Table List
    query =  "SELECT table_name FROM information_schema.tables WHERE table_type = \'base table\' AND table_schema=\'{0}\';".format(backup_mysql_schema)
    cmd = 'mysql -N -s -u {0}  -p{1} -e "{2}" > tables '.format(backup_mysql_user, backup_mysql_pass, query)
    output = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    t.upload_file('tables', backup_aws_bucket, 'tables')
  except:
    msg = "An error ocurred while uploading the backup"
    logger.error(msg, exc_info)
    send_email(msg)
    exit(1)
    raise
  logger.info("Post Backup Finished")
  logger.info('Files Uploaded:')
  logger.info(backup_mysql_filename_schema)
  logger.info(backup_mysql_filename_data)
  logger.info(backup_mysql_md5sum_data)
  logger.info(backup_mysql_md5sum_schema)
  return True


# we are going to use the values from the Json itself, to simplify the code
json_data = parse_config(args.config_file)

backup_date = datetime.today().strftime('%Y%m%d%H%M%S')
backup_mysql_user = json_data['backup']['mysql']['user']
backup_mysql_pass = json_data['backup']['mysql']['pass']
backup_mysql_schema = json_data['backup']['mysql']['schema']
backup_mysql_filename_schema = backup_mysql_schema+"."+backup_date+".sql.gz"
backup_mysql_filename_data = backup_mysql_schema+"."+backup_date+"_data.sql.gz"
backup_mysql_md5sum_schema = "md5sum_"+backup_mysql_schema+"."+backup_date
backup_mysql_md5sum_data = "md5sum_"+backup_mysql_schema+"."+backup_date+"_data"
backup_aws_s3key = json_data['backup']['aws']['access_key']
backup_aws_secret = json_data['backup']['aws']['secret']
backup_aws_bucket = json_data['backup']['aws']['bucket']


logger.info('Execution {0} started, parameters shared: {1}'.format(backup_date, args))


if args.backup:
  logger.info('Requested Backup Operation...')
  pre_backup_health_check()
  backup()
  post_backup()
  msg = 'Backup Operation finished'
  logger.info(msg)
  send_email(msg)
