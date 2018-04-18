from contextlib import closing
import socket
import sys
import paramiko
import time
from send_test import RetrieveClosedPorts
from deploy_EC2 import DeployEC2

class RapidDeployer(object):

  def __init__(self):
    self.seedIPAddr = []
    self.seedPorts = []
    self.iterator = None

  # def find_open_ports(self, IP_Addr):
  #   for port in range(1025, 32000):
  #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  #     result = sock.connect_ex((IP_Addr, port))
  #     if result != 0:  
  #       yield port        

  # print next(find_open_ports('localhost'))
  def get_localIP(self, hostname, username, key_filename):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, username=username, key_filename=key_filename)
    stdin, stdout, stderr = ssh.exec_command("ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'")
    local_ip = stdout.readlines()
    local_ip = [x.strip('\n') for x in local_ip]
    return local_ip[0]

  def execute_cmd(self, hostname, username, key_filename, command):

    # command = "nohup python /home/kartik/Development/rapid_tests/hello.py > /home/kartik/Development/rapid_tests/nohup.out &> /home/kartik/Development/rapid_tests/nohup.err &"
    print("Command to send: {}".format(command))

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, username=username, key_filename=key_filename)

    stdin, stdout, stderr = ssh.exec_command(command)
    print(stdout.read())
    ssh.close()

  def add_starter_nodes(self, IP_Addr, username, key_filename, start_port, end_port, no_of_processes):
    
    global_IP = IP_Addr
    IP_Addr = self.get_localIP(global_IP, username, key_filename)

    listenAddress = IP_Addr[:]
    
    for _ in xrange(no_of_processes):
      self.seedIPAddr.append(IP_Addr[:])

    seedAddress = IP_Addr[:]
    
    y = RetrieveClosedPorts(global_IP, username, key_filename, start_port, end_port)

    ports = y.retrieve()
    ports = ports[:no_of_processes]
    
    for seedPort in ports:
      # print seedPort
      self.seedPorts.append(seedPort)
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/EC2_Files/rapid-examples-1.0-SNAPSHOT-allinone.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(seedPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"starter\" > ~/EC2_Files/" + IP_Addr + "_" + str(seedPort) + ".log 2>~/EC2_Files/" + IP_Addr + "_" + str(seedPort) + ".err &"
      
      self.execute_cmd(global_IP, username, key_filename, command)

      
  def add_joiner_node(self, IP_Addr, seedAddress, seedPort, username, key_filename, start_port, end_port, no_of_processes):

    global_IP = IP_Addr
    IP_Addr = self.get_localIP(global_IP, username, key_filename)
    listenAddress = IP_Addr

    y = RetrieveClosedPorts(global_IP, username, key_filename, start_port, end_port)

    ports = y.retrieve()
    ports = ports[:no_of_processes]
    
    for listenPort in ports:    
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/EC2_Files/rapid-examples-1.0-SNAPSHOT-allinone.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(listenPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"joiner\" > ~/EC2_Files/" + IP_Addr + "_" + str(seedPort) + ".log 2>~/EC2_Files/" + IP_Addr + "_" + str(listenPort) + ".err &"
      
      self.execute_cmd(global_IP, username, key_filename, command)
    
x = RapidDeployer()

y = DeployEC2(2, 't2.micro', 'rapidkey')
instance_ids = y.spawn_instances()
IPs = y._get_instance_IPs(instance_ids)
y.send_Files(instance_ids, "ubuntu", "/home/kartik/Development/rapidkey.pem")



# Uncomment and Fill in with hostname, username and key_filename 
x.add_starter_nodes(IPs[0], "ubuntu", "/home/kartik/Development/rapidkey.pem", 7000, 7500, 2)
time.sleep(20)
x.add_joiner_node(IPs[0], x.seedIPAddr[0], x.seedPorts[0], "ubuntu", "/home/kartik/Development/rapidkey.pem", 7000, 7500, 2)
time.sleep(20)
x.add_joiner_node(IPs[1], x.seedIPAddr[1], x.seedPorts[1], "ubuntu", "/home/kartik/Development/rapidkey.pem", 7000, 7500, 2)
# time.sleep(20)
# x.add_joiner_node("", x.seedIPAddr[2], x.seedPorts[2], "", "", 7000, 7500, 2)
# while(1):
#   print "Options."
#   print "1. Deploy Instances"
#   print "2. Add starter nodes"
#   print "3. Add joiner nodes"
#   inp = raw_input()
#   if inp == 'Exit':
#     break


