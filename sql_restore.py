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
# # Instead of tedious parameters that can end up messing, I found a file to define all the possible settings simplier and easier to extent
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
mxg.add_argument('-r', '--restore', default=False, action="store_true", help='Start restore')

args = parser.parse_args()

def send_email(msg):
  port = 465  #  For SSL
  smtp_server = "smtp.gmail.com"
  sender_email = "bvcelari@gmail.com"  #  Enter your address
  receiver_email = "bvcelari@gmail.com"  #  Enter receiver address
  # You need to enable it in google https://www.google.com/settings/security/lesssecureapps
  password = "Your password here"
  message = '''
Subject: Restore Notification 
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

def pre_restore():
  # check source of the files in AWS: dump and checksum
  logger.info("Pre Restore Health Check Starting... ")
  try:
    logger.info("Checking AWS S3... ")
    session = Session(aws_access_key_id=restore_aws_s3key,
      aws_secret_access_key=restore_aws_secret,
    )
    s3 = session.resource('s3')
    your_bucket = s3.Bucket(restore_aws_bucket)
    has_schema, has_data = False, False
    for s3_file in your_bucket.objects.all():
      if s3_file.key == restore_aws_filedata:
        logger.info("We have the bucket: {0}, and the file {1}".format(restore_aws_bucket, restore_aws_filedata))
        has_data = True
      if s3_file.key == restore_aws_fileschema:
        logger.info("We have the bucket: {0}, and the file {1}".format(restore_aws_bucket, restore_aws_fileschema))
        has_schema = True
      if has_data == True and has_schema == True:
        break
    else:
      msg = "Cannot find the bucket: {0}, all the required files".format(restore_aws_bucket)
      logger.error(msg)
      send_email(msg)
      exit(1)
  except:
    msg = "An error ocurred while checking aws "
    logger.error(msg, exc_info=True)
    send_email(msg)
    exit(1)
  logger.info("AWS S3 Ok")

  # download files and check integrity
  try:
    logger.info("Downloading files Starting... ")
    logger.info("downloading "+restore_aws_fileschema)
    s3.Bucket(restore_aws_bucket).download_file(restore_aws_fileschema, restore_aws_fileschema)
    logger.info("downloading "+restore_aws_filedata)
    s3.Bucket(restore_aws_bucket).download_file(restore_aws_filedata, restore_aws_filedata)
    logger.info("downloading "+restore_mysql_md5sum_data)
    s3.Bucket(restore_aws_bucket).download_file(restore_mysql_md5sum_data, restore_mysql_md5sum_data)
    logger.info("downloading "+restore_mysql_md5sum_schema)
    s3.Bucket(restore_aws_bucket).download_file(restore_mysql_md5sum_schema, restore_mysql_md5sum_schema)
    logger.info("downloading tables")
    s3.Bucket(restore_aws_bucket).download_file('tables', 'tables')
  except:
    msg = "An error ocurred while downloading the files ... "
    logger.error(msg, exc_info=True)
    send_email(msg)
    exit(1)
  logger.info("Downloading files Finished")

  logger.info("Unziping files starting... ")
  cmd_sql_trick = 'mysql -u {0}  -p{1} -e \'SET AUTOCOMMIT = 0; SET FOREIGN_KEY_CHECKS = 0  ;\''.format(restore_mysql_user, restore_mysql_pass)
  output_sql_trick = subprocess.call(cmd_sql_trick, shell=True)
  if output_sql_trick != 0:
    msg = "error checking the DB with the command: {0}".format(cmd_sql_trick)
    logger.error(msg)
    send_email(msg)
    exit(1)
  logger.info("Gunziping Finished")
  for i in [restore_aws_filedata, restore_aws_fileschema]:
    logger.info("Unziping "+i)
    cmd_gunzip = 'gunzip '+i
    output_cmd_gunzip = subprocess.call(cmd_gunzip, shell=True)
    if output_cmd_gunzip != 0:
      msg = "error gunziping: {0}".format(cmd_gunzip)
      logger.error(msg)
      send_email(msg)
      exit(1)
  logger.info("Unziiping files starting... ")
  logger.info("Checking Mysql... ")
  # check DB connectivity (data?)
  # You need to check user, pass and schema to mysql, I am assuming here that mysql cmd exist
  cmd = 'mysql -u {0}  -p{1} -e \'use {2}  ;\''.format(restore_mysql_user, restore_mysql_pass, restore_mysql_schema)
  check_mysql = subprocess.call(cmd, shell=True)
  if check_mysql != 0:
    logger.error("error checking the DB with the command: {0}".format(cmd))
    send_email(msg)
    exit(1)
  logger.info("Mysql Ok")
  # check space enough for the task   


def restore():
  logger.info("Restore Starting... ")
  # run the Improve performance steps
  # Disabling:
  #  SET AUTOCOMMIT = 0; SET FOREIGN_KEY_CHECKS=0
  #  --max_allowed_packet=256M
  #  innodb_buffer_pool_size 
  #  innodb_flush_log_at_trx_commit
  #  innodb_flush_method
  logger.info("Disabling Fk and autocommint... ")
  cmd_sql_trick = 'mysql -u {0}  -p{1} -e \'SET AUTOCOMMIT = 0; SET FOREIGN_KEY_CHECKS = 0  ;\''.format(restore_mysql_user, restore_mysql_pass)
  output_sql_trick = subprocess.call(cmd_sql_trick, shell=True)
  if output_sql_trick != 0:
    msg = "error checking the DB with the command: {0}".format(cmd_sql_trick)
    logger.error(msg)
    send_email(msg)
    exit(1)
  logger.info("Disabling Finished")
  # If mysql is higher than 5.7.5 you can modify the inmodb settings wihtout restart, like: SET GLOBAL innodb_buffer_pool_size=402653184;

  # restore, we assume the DB is created, I am not going to drop a DB of this size in a script wihtout more information
  logger.info("Restoring schema started... ")
  cmd_import_schema= 'mysql -u {0}  -p{1} {2} < {3} '.format(restore_mysql_user, restore_mysql_pass, restore_mysql_schema, restore_aws_fileschema.split('.gz')[0])
  output_import_schema = subprocess.call(cmd_import_schema, shell=True)
  if output_import_schema != 0:
    msg = "error checking the DB with the command: {0}".format(cmd_import_schema)
    logger.error(msg)
    send_email(msg)
    exit(1)
  logger.info("Schema Finished")

  #  mysql -u username -p new_database < data-dump.sql
  logger.info("Restoring data started... ")
  cmd_import_data= 'mysql -u {0} -p{1} {2} < {3} '.format(restore_mysql_user, restore_mysql_pass, restore_mysql_schema, restore_aws_filedata.split('.gz')[0])
  output_import_data = subprocess.call(cmd_import_data, shell=True)
  if output_import_data != 0:
    msg = "error checking the DB with the command: {0}".format(cmd_import_data)
    logger.error(msg)
    send_email(msg)
    exit(1)
  logger.info("Data Finished")

  # rollback the performance changes
  logger.info("Enabling Fk and autocomint... ")
  cmd_sql_trick_rb = 'mysql -u {0}  -p{1} -e \'SET AUTOCOMMIT = 1; SET FOREIGN_KEY_CHECKS = 1  ;\''.format(restore_mysql_user, restore_mysql_pass)
  output_sql_trick_rb = subprocess.call(cmd_sql_trick_rb, shell=True)
  if output_sql_trick_rb != 0:
    msg = "error checking the DB with the command: {0}".format(cmd_sql_trick_rb)
    logger.error(msg)
    send_email(msg)
    exit(1)
  logger.info("Enabling Finished")

def post_restore():
  # check size or known data in the DB
  # check the header and the tail of the dump
  #  Check tables:
  # #  SELECT CONCAT('CHECK TABLE ',dbtb,';') FROM (SELECT CONCAT(table_schema,'.',table_name) dbtb FROM information_schema.tables WHERE table_schema IN ('employees2')) A;
  # #  That should give you the tables in the schema
  query =  "SELECT table_name FROM information_schema.tables WHERE table_type = \'base table\' AND table_schema=\'{0}\';".format(restore_mysql_schema)
  cmd = 'mysql -N -s -u {0}  -p{1} -e "{2}" > tables_restore '.format(restore_mysql_user, restore_mysql_pass, query)
  output = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True)
  # les's compare the tables file downlaoded and the tables_restore file, should be identical
  cmd = 'diff tables tables_restore'
  output = subprocess.call(cmd, shell=True)
  subprocess.call(cmd, shell=True)
  if output != 0:
    logger.error("Error has been found, the schemas does not have the same amount of tables")


# we are going to use the values from the Json as input data, instead of a bunch of parameters
json_data = parse_config(args.config_file)

restore_date = datetime.today().strftime('%Y%m%d%H%M%S')
restore_mysql_user = json_data['restore']['mysql']['user']
restore_mysql_pass = json_data['restore']['mysql']['pass']
restore_mysql_schema = json_data['restore']['mysql']['schema']
restore_aws_s3key = json_data['restore']['aws']['access_key']
restore_aws_secret = json_data['restore']['aws']['secret']
restore_aws_bucket = json_data['restore']['aws']['bucket']
restore_aws_filedata = json_data['restore']['aws']['filedata']
restore_aws_fileschema = json_data['restore']['aws']['fileschema']
restore_mysql_md5sum_schema =  json_data['restore']['aws']['md5schema']
restore_mysql_md5sum_data = json_data['restore']['aws']['md5data']



logger.info('Execution {0} started, parameters shared: {1}'.format(restore_date, args))


if args.restore:
  logger.info('Requested Restore Operation... ')
  pre_restore()
  restore()
  post_restore()
  msg = 'Restore Operation finished'
  logger.info(msg)
  send_mail(msg)



