import paramiko
import sys

class RetrieveClosedPorts(object):
  def __init__(self, hostname, username, password, port_start, port_end):
    self.hostname = hostname
    self.username = username
    self.password = password
    self.port_start = port_start
    self.port_end = port_end

  def retrieve(self):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=self.hostname, username=self.username, password=self.password)

    ftp_client=ssh.open_sftp()
    ftp_client.put('/home/kartik/Development/rapid_tests/scan_ports.sh','/u/kartik/Development/rapid_tests/scan_ports2.sh')
    ftp_client.close()

    stdin, stdout, stderr = ssh.exec_command("/bin/bash /u/kartik/Development/rapid_tests/scan_ports2.sh " + self.hostname + " " + str(self.port_start) + " " + str(self.port_end))

    l = stdout.readlines()
    l = [int(x.strip('\n')) for x in l]

    return l

# x = RetrieveClosedPorts(sys.argv[1], sys.argv[2], sys.argv[3])
# x.retrieve()