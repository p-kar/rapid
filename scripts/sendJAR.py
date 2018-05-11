import paramiko
import os
import argparse
import sys
from threading import Thread

parser = argparse.ArgumentParser()

parser.add_argument("-pub", "--public-ips", help='list of publicIPs', required=True, type=str, nargs='+')
parser.add_argument("-file", "--filename", help='path to Rapid Jar', required=True, type=str)
opts = parser.parse_args()
publicIPs = opts.public_ips
filename = opts.filename

def sendRapidJar(hostname, username, key_filename, filename):
  ssh_client = paramiko.SSHClient()
  ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh_client.connect(hostname=hostname,username=username,key_filename=key_filename)

  ftp_client = ssh_client.open_sftp()
  ftp_client.put(filename, "/home/cc/tools.py")
  ftp_client.close()

  print "Sent JAR to", hostname

# list_threads = []
# for ip in publicIPs:
#   t = Thread(target=sendRapidJar, args=(ip, "cc", "/home/kartik/Development/chamkey.pem", filename))
#   list_threads.append(t)
#   t.start()
#   # sendRapidJar(ip, "cc", "/home/kartik/Development/chamkey.pem", filename)
# [t.join() for t in list_threads]

for ip in publicIPs:
  sendRapidJar(ip, "cc", "/home/kartik/Development/chamkey.pem", filename)