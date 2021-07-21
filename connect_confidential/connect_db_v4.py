import pymysql
import pymysql.cursors
import paramiko
from os.path import expanduser
from sshtunnel import SSHTunnelForwarder

home = expanduser('~')
pkeyfilepath = '/Documents/VIBBIDI/DBconnect/ec2-proxy2-glue-th.pem'
mypkey = paramiko.RSAKey.from_private_key_file(home + pkeyfilepath)
# if you want to use ssh password use - ssh_password='your ssh password', bellow

sql_hostname = 'v4-mysql-master.vibbidi.com'
sql_username = 'sysadm'
sql_password = 'ek7F7ck3'
sql_main_database = 'v4'
sql_port = 3306
ssh_host = 'db-proxy.vibbidi.net'
ssh_user = 'ec2-user'
ssh_port = 22
sql_ip = '1.1.1.1.1'

tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(sql_hostname, sql_port))
tunnel.start()
conn = pymysql.connect(host='127.0.0.1', user=sql_username, #connect to the database
        passwd=sql_password, db=sql_main_database,
        port=tunnel.local_bind_port)
cursor = conn.cursor()