import pymysql
import paramiko
from os.path import expanduser
# import calendar

home = expanduser('~')
pkeyfilepath = '/Documents/VIBBIDI/DBconnect/ec2-proxy2-glue-th.pem'
mypkey = paramiko.RSAKey.from_private_key_file(home + pkeyfilepath)
# if you want to use ssh password use - ssh_password='your ssh password', bellow

sql_hostname = 'v4-mysql-master.vibbidi.com'
sql_username = 'sysadm'
sql_password = 'ek7F7ck3'
# sql_main_database = 'original_social_graph'
sql_port = 3306
ssh_host = 'db-proxy.vibbidi.net'
ssh_user = 'ec2-user'
ssh_port = 22
sql_ip = '1.1.1.1.1'

