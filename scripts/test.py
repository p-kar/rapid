from contextlib import closing
import socket
import sys
import paramiko
import time
from send_test import RetrieveClosedPorts

class RapidDeployer(object):

  def __init__(self):
    self.seedIPAddr = []
    self.seedPorts = []
    self.iterator = None

  def find_open_ports(self, IP_Addr):
    for port in range(1025, 32000):
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      result = sock.connect_ex((IP_Addr, port))
      if result != 0:  
        yield port        

  # print next(find_open_ports('localhost'))

  def execute_cmd(self, hostname, username, password, command):

    # command = "nohup python /home/kartik/Development/rapid_tests/hello.py > /home/kartik/Development/rapid_tests/nohup.out &> /home/kartik/Development/rapid_tests/nohup.err &"
    print("Command to send: {}".format(command))

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, username=username, password=password)

    stdin, stdout, stderr = ssh.exec_command(command)
    print(stdout.read())
    ssh.close()

  def add_starter_nodes(self, IP_Addr, username, password, start_port, end_port, no_of_processes):
    
    listenAddress = IP_Addr[:]
    
    for _ in xrange(no_of_processes):
      self.seedIPAddr.append(IP_Addr[:])

    seedAddress = IP_Addr[:]
    
    y = RetrieveClosedPorts(IP_Addr, username, password, start_port, end_port)

    ports = y.retrieve()
    ports = ports[:no_of_processes]
    
    for seedPort in ports:
      # print seedPort
      self.seedPorts.append(seedPort)
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/Development/rapid_tests/rapid/rapid-examples/target/rapid-examples-1.0-SNAPSHOT-allinone.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(seedPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"starter\" > ~/Development/rapid_tests/" + IP_Addr + "_" + str(seedPort) + ".log 2>~/Development/rapid_tests/" + IP_Addr + "_" + str(seedPort) + ".err &"
      
      self.execute_cmd(IP_Addr, username, password, command)

      
  def add_joiner_node(self, IP_Addr, seedAddress, seedPort, username, password, start_port, end_port, no_of_processes):

    listenAddress = IP_Addr

    y = RetrieveClosedPorts(IP_Addr, username, password, start_port, end_port)

    ports = y.retrieve()
    ports = ports[:no_of_processes]
    
    for listenPort in ports:    
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/Development/rapid_tests/rapid/rapid-examples/target/rapid-examples-1.0-SNAPSHOT-allinone.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(listenPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"joiner\" > ~/Development/rapid_tests/" + IP_Addr + "_" + str(seedPort) + ".log 2>~/Development/rapid_tests/" + IP_Addr + "_" + str(listenPort) + ".err &"
      
      self.execute_cmd(IP_Addr, username, password, command)

    
x = RapidDeployer()

# Uncomment and Fill in with hostname, username and password 
# x.add_starter_nodes("", "", "", 7000, 7500, 3)
# time.sleep(10)
# x.add_joiner_node("", x.seedIPAddr[0], x.seedPorts[0], "", "", 7000, 7500, 2)
# time.sleep(20)
# x.add_joiner_node("", x.seedIPAddr[1], x.seedPorts[1], "", "", 7000, 7500, 2)
# time.sleep(20)
# x.add_joiner_node("", x.seedIPAddr[2], x.seedPorts[2], "", "", 7000, 7500, 2)