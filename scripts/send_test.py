import paramiko
import sys

class RetrieveClosedPorts(object):
  def __init__(self, hostname, username, key_filename, port_start, port_end):
    self.hostname = hostname
    self.username = username
    self.key_filename = key_filename
    self.port_start = port_start
    self.port_end = port_end

  def retrieve(self):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=self.hostname, username=self.username, key_filename=self.key_filename)

    # ftp_client=ssh.open_sftp()
    # ftp_client.put('/home/kartik/Development/rapid_tests/scan_ports.sh','/u/kartik/Development/rapid_tests/scan_ports2.sh')
    # ftp_client.close()
    stdin, stdout, stderr = ssh.exec_command("ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'")
    local_ip = stdout.readlines()
    local_ip = [x.strip('\n') for x in local_ip]
    local_ip = local_ip[0]
    stdin, stdout, stderr = ssh.exec_command("/bin/bash /home/ubuntu/EC2_Files/scan_ports.sh " + str(local_ip) + " " + str(self.port_start) + " " + str(self.port_end))

    l = stdout.readlines()
    l = [int(x.strip('\n')) for x in l]

    return l

# x = RetrieveClosedPorts(sys.argv[1], sys.argv[2], sys.argv[3])
# x.retrieve()