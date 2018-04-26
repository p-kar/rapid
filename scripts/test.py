import sys
import paramiko
import time
import random
import os
from threading import Thread
from datetime import datetime
from deploy_EC2 import DeployEC2

class RapidDeployer(object):

  def __init__(self):
    self.seedIPAddr = []
    self.seedPorts = []
    self.joinIPAddr = []
    self.iterators = {}

  def find_open_ports(self, IP_Addr):
    for port in range(9000, 32000):
        yield port        

  # print next(find_open_ports('localhost'))
  def get_localIP(self, hostname, username, key_filename):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, username=username, key_filename=key_filename)
    stdin, stdout, stderr = ssh.exec_command("ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'")
    local_ip = stdout.readlines()
    print local_ip
    local_ip = [x.strip('\n') for x in local_ip]
    return local_ip[0]

  def execute_cmd(self, hostname, username, key_filename, command):

    # command = "nohup python /home/kartik/Development/rapid_tests/hello.py > /home/kartik/Development/rapid_tests/nohup.out &> /home/kartik/Development/rapid_tests/nohup.err &"
    print("Command to send: {}".format(command))

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, username=username, key_filename=key_filename)

    stdin, stdout, stderr = ssh.exec_command(command)
    # print("Command complete: {}".format(command))
    ssh.close()

  def add_starter_nodes(self, global_IP, IP_Addr, username, key_filename, no_of_processes):
    
    # global_IP = IP_Addr
    # IP_Addr = self.get_localIP(global_IP, username, key_filename)
    listenAddress = IP_Addr
    
    for _ in xrange(no_of_processes):
      self.seedIPAddr.append(IP_Addr[:])

    seedAddress = IP_Addr[:]

    ports = []
    if global_IP not in self.iterators.keys():
      self.iterators[global_IP] = self.find_open_ports(global_IP)
    
    port_getter = self.iterators[global_IP]
    for _ in xrange(no_of_processes):
      ports.append(port_getter.next())
    
    print "Ports :", ports
    list_threads = []

    for seedPort in ports:
    #   # print seedPort
      self.seedPorts.append(seedPort)
      '''
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/rapid-examples-1.0-SNAPSHOT-allinone.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(seedPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"starter\" > ~/" + IP_Addr + "_" + str(seedPort) + ".log 2>~/" + IP_Addr + "_" + str(seedPort) + ".err &"
      '''
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/rapid-lalith.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(seedPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"starter\" > ~/" + IP_Addr + "_" + str(seedPort) + ".log 2>~/" + IP_Addr + "_" + str(seedPort) + ".err &"
      t = Thread(target=self.execute_cmd, args=(global_IP, username, key_filename, command) )
      list_threads.append(t)
      t.start()

    [t.join() for t in list_threads]

      
  def add_joiner_node(self, global_IP, IP_Addr, seedAddress, seedPort, username, key_filename, no_of_processes):

    # global_IP = IP_Addr
    # IP_Addr = self.get_localIP(global_IP, username, key_filename)
    listenAddress = IP_Addr

    ports = []
    if global_IP not in self.iterators.keys():
      self.iterators[global_IP] = self.find_open_ports(global_IP)
    
    port_getter = self.iterators[global_IP]
    for _ in xrange(no_of_processes):
      ports.append(port_getter.next())
    
    print "Ports :", ports
    list_threads = []

    for listenPort in ports:
      '''    
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/rapid-examples-1.0-SNAPSHOT-allinone.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(listenPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"joiner\" > ~/" + IP_Addr + "_" + str(listenPort) + ".log 2>~/" + IP_Addr + "_" + str(listenPort) + ".err &"
      '''
      command = "nohup java -server -Xms50m -Xmx50m -jar ~/rapid-lalith.jar --listenAddress " + \
                "\"" + str(listenAddress) + ":" + str(listenPort) + "\" --seedAddress " + "\"" + str(seedAddress) + ":" + str(seedPort) + "\" \
                --cluster Rapid --role \"joiner\" > ~/" + IP_Addr + "_" + str(listenPort) + ".log 2>~/" + IP_Addr + "_" + str(listenPort) + ".err &"
      t = Thread(target=self.execute_cmd, args=(global_IP, username, key_filename, command) )
      self.joinIPAddr.append((global_IP, listenAddress, listenPort))
      list_threads.append(t)
      t.start()

    [t.join() for t in list_threads]
      # self.joinPorts.append(listenPort)

  def kill_process(self, global_IP, local_IP, Port, username, key_filename):
    self.execute_cmd(global_IP, username, key_filename, "pkill -f \"listenAddress " + str(local_IP) + ":" + str(Port) + "\"")
    
  def kill_all_processes(self, global_IP, username, key_filename):
    self.execute_cmd(global_IP, username, key_filename, "pkill -f \"rapid\"")

  def getLogs(self, global_IP):
    timeStamp = datetime.now().strftime('%Y-%m-%d+%H:%M:%S')
    os.system("mkdir -p /home/kartik/Development/rapid_tests/logs/" + global_IP + "+" + timeStamp)
    os.system("scp -i /home/kartik/Development/rapidkey.pem ec2-user@" + global_IP + ":/home/ec2-user/*.log /home/kartik/Development/rapid_tests/logs/" + global_IP + "+" + timeStamp)
    os.system("scp -i /home/kartik/Development/rapidkey.pem ec2-user@" + global_IP + ":/home/ec2-user/*.err /home/kartik/Development/rapid_tests/logs/" + global_IP + "+" + timeStamp)

  def inject_asymmetric_drops(self, global_IP, Port, username, key_filename):
    self.execute_cmd(global_IP, username, key_filename, "sudo iptables -A INPUT -j DROP -p tcp --destination-port " + str(Port))
    time.sleep(18)
    self.execute_cmd(global_IP, username, key_filename, "sudo iptables -A INPUT -j ACCEPT -p tcp --destination-port " + str(Port))
    time.sleep(18)
    self.execute_cmd(global_IP, username, key_filename, "sudo iptables -A INPUT -j DROP -p tcp --destination-port " + str(Port))
    time.sleep(18)
    self.execute_cmd(global_IP, username, key_filename, "sudo iptables -A INPUT -j ACCEPT -p tcp --destination-port " + str(Port))
# y = RapidDeployer()

# # instances_nos = 20
# # instance_ids = []
# # x1 = DeployEC2(10, 't2.large', 'rapidkey')
# # for _ in xrange(instances_nos/10 - 1):

# x1 = DeployEC2(5, 'c5.2xlarge', 'rapidkey')
# # x1.spawn_instances()
# instance_ids = x1._get_instance_IDs()
# #  instance_ids.append(x1.spawn_instances())
# # x.send_Files(instance_ids, "ec2-user", "/home/kartik/Development/rapidkey.pem")
# print "Instance IDs :", instance_ids
# # instance_ids = ['i-002751df1b084e269', 'i-0084ab694cae853e9', 'i-013b5e6896330fdd7', 'i-02899f36cc00f02c3', 
# # 'i-02f0978589c699592', 'i-0338d6b780c29dc99', 'i-05863e5f8c1945bb4', 'i-06f1f8d231508021c', 
# # 'i-0915b8d13b1d4311f', 'i-0e65eaf86d1b845c7']
# publicIPs, privateIPs = x1._get_instance_IPs(instance_ids)
# print "IP addresses : ", publicIPs, privateIPs

# # y.kill_process(publicIPs[1], privateIPs[1], "9001", "ec2-user", "/home/kartik/Development/rapidkey.pem")

# # Uncomment and Fill in with hostname, username and key_filename 
# y.add_starter_nodes(publicIPs[0], privateIPs[0], "ec2-user", "/home/kartik/Development/rapidkey.pem", 1)
# time.sleep(5)
# # y.add_joiner_node(publicIPs[1], privateIPs[1], y.seedIPAddr[0], y.seedPorts[0], "ec2-user", "/home/kartik/Development/rapidkey.pem", 3)
# # time.sleep(20)
# # y.add_joiner_node(publicIPs[2], privateIPs[2], y.seedIPAddr[0], y.seedPorts[0], "ec2-user", "/home/kartik/Development/rapidkey.pem", 3)
# # for i in xrange(10):
# #    y.add_joiner_node(publicIPs[i], privateIPs[i], y.seedIPAddr[0], y.seedPorts[0], "ec2-user", "/home/kartik/Development/rapidkey.pem", 10)
# #   time.sleep(2)

# for i in xrange(5):
#   y.add_joiner_node(publicIPs[i], privateIPs[i], y.seedIPAddr[0], y.seedPorts[0], "ec2-user", "/home/kartik/Development/rapidkey.pem", 5)
# # time.sleep(20)
# # x.add_joiner_node("", x.seedIPAddr[2], x.seedPorts[2], "", "", 7000, 7500, 2)

y = RapidDeployer()
x = DeployEC2()

instance_ids = x._get_instance_IDs()
publicIPs, privateIPs = x._get_instance_IPs(instance_ids)

for ip in publicIPs:
  try:
    y.execute_cmd(ip, "ec2-user", "/home/kartik/Development/rapidkey.pem", "rm -rf /home/ec2-user/*.log && rm -rf /home/ec2-user/*.err")
    y.execute_cmd(ip, "ec2-user", "/home/kartik/Development/rapidkey.pem", "nohup python measure.py > " + ip + ".log 2&>" + ip + ".err &")
  except Exception as ex:
    print ex

while(1):
  print "Options."
  print "1. Deploy Instances"
  print "2. Add starter nodes"
  print "3. Add joiner nodes"
  print "4. Kill process"
  print "5. Kill All processes"
  print "6. Print running processes"
  print "7. Stop all EC2 instances"
  print "8. Terminate all EC2 instances"
  print "9. Retrieve Logs"
  print "10. Reset logs and bandwidth measurement"
  print "11. Inject Asymmetric packet drops"
  print "12. Exit"
  
  inp = raw_input()
  
  if inp == 'Exit':
    break
  
  elif inp == '1':
    N, instance_type, key_name = raw_input().split()
    n = int(N)
    x = DeployEC2(n, instance_type, key_name)
    
    try:
      x.spawn_instances()
    except Exception as ex:
      print ex
    
    instance_ids = x._get_instance_IDs()
    print "Instance IDs :", instance_ids
    publicIPs, privateIPs = x._get_instance_IPs(instance_ids)
    time.sleep(30)
    for ip in publicIPs:
      try:
        y.execute_cmd(ip, "ec2-user", "/home/kartik/Development/rapidkey.pem", "rm -rf /home/ec2-user/*.log && rm -rf /home/ec2-user/*.err")
        y.execute_cmd(ip, "ec2-user", "/home/kartik/Development/rapidkey.pem", "nohup python measure.py > " + ip + ".log 2&>" + ip + ".err &")
      except Exception as ex1:
        print ex1
    

    print "Deploy Finished"

  elif inp == '2':
    N = int(raw_input())
    instance_ids = x._get_instance_IDs()
    publicIPs, privateIPs = x._get_instance_IPs(instance_ids)
    try:
      y.add_starter_nodes(publicIPs[0], privateIPs[0], "ec2-user", "/home/kartik/Development/rapidkey.pem", N)
    except Exception as ex:
      print ex

    time.sleep(5)
    print "Starter node spawned"

  elif inp == '3':
    no_of_processes, no_of_nodes = raw_input().split()
    no_of_processes = int(no_of_processes)
    no_of_nodes = int(no_of_nodes)
    publicIPs, privateIPs = x._get_instance_IPs(instance_ids)
    try:
      for i in xrange(min(no_of_nodes, len(publicIPs))):
        y.add_joiner_node(publicIPs[i], privateIPs[i], y.seedIPAddr[0], y.seedPorts[0], "ec2-user", "/home/kartik/Development/rapidkey.pem", no_of_processes)
    except Exception as ex:
      print ex

    time.sleep(10)
    print "Joiner node spawned"

  elif inp == '4':
    N = int(raw_input())
    instance_ids = x._get_instance_IDs()
    list_threads = []
    for i in xrange(N):
      rand_gen = random.randint(0, len(y.joinIPAddr) - 1)
      try:
        del y.joinIPAddr[rand_gen]
        t = Thread(target=y.kill_process, args=(y.joinIPAddr[rand_gen][0], y.joinIPAddr[rand_gen][1], y.joinIPAddr[rand_gen][2], "ec2-user", "/home/kartik/Development/rapidkey.pem") )
        list_threads.append(t)
        t.start()
        
      except Exception as ex:
        print ex

    [t.join() for t in list_threads]

  elif inp == '5':
    instance_ids = x._get_instance_IDs()
    publicIPs, privateIPs = x._get_instance_IPs(instance_ids)
    list_threads = []
    for i in xrange(len(publicIPs)):
      try:
        t = Thread(target=y.kill_all_processes, args=(publicIPs[i], "ec2-user", "/home/kartik/Development/rapidkey.pem") )
        list_threads.append(t)
        t.start()
        # y.execute_cmd(publicIPs[i], "ec2-user", "/home/kartik/Development/rapidkey.pem", "rm -rf /home/ec2-user/*.log && rm -rf /home/ec2-user/*.err")
      except Exception as ex:
        print ex

    [t.join() for t in list_threads]
    del y.seedIPAddr[:]
    del y.seedPorts[:]
    del y.joinIPAddr[:]

  elif inp == '6':
    try :
      print "Seed Address :" + ' ' + str(y.seedIPAddr[0]) + ':' + str(y.seedPorts[0])
    except Exception as ex:
      print ex
      continue

    print "Joiner Processes :"
    try:
      for i in xrange(len(y.joinIPAddr)):
        print str(y.joinIPAddr[i][0]) + ':' + str(y.joinIPAddr[i][2])
    except Exception as ex:
      print ex

  elif inp == '7':
    try:
      instance_ids = x._get_instance_IDs()
      print "Stopping Instance IDs :", instance_ids
      x.stop_instances(instance_ids)
    except Exception as ex:
      print ex

  elif inp == '8':
    try:
      instance_ids = x._get_instance_IDs()
      print "Terminating Instance IDs :", instance_ids
      x.terminate_instances(instance_ids)
    except Exception as ex:
      print ex

  elif inp == '9':
    
    instance_ids = x._get_instance_IDs()
    publicIPs, privateIPs = x._get_instance_IPs(instance_ids)
    list_threads = []

    for ip in publicIPs:
      try:
        t = Thread(target=y.getLogs, args=(ip))
        list_threads.append(t)
        t.start()
      except Exception as ex:
        print ex

    [t.join() for t in list_threads]

  elif inp == '10':
    instance_ids = x._get_instance_IDs()
    publicIPs, privateIPs = x._get_instance_IPs(instance_ids)
    list_threads = []

    for ip in publicIPs:
      try:
        t = Thread(target=y.execute_cmd, args=(ip, "ec2-user", "/home/kartik/Development/rapidkey.pem", "rm -rf /home/ec2-user/*.log && rm -rf /home/ec2-user/*.err && nohup python measure.py > " + ip + ".log 2&>" + ip + ".err &") )
        list_threads.append(t)
        t.start()
        # y.execute_cmd(ip, "ec2-user", "/home/kartik/Development/rapidkey.pem", "nohup python measure.py > " + ip + ".log 2&>" + ip + ".err &")
      except Exception as ex:
        print ex

    [t.join() for t in list_threads]

  elif inp == '11':
    N = int(raw_input())
    instance_ids = x._get_instance_IDs()
    list_threads = []

    for i in xrange(N):
      rand_gen = random.randint(0, len(y.joinIPAddr) - 1)
      try:
        t = Thread(target=y.inject_asymmetric_drops, args=(y.joinIPAddr[rand_gen][0], y.joinIPAddr[rand_gen][2], "ec2-user", "/home/kartik/Development/rapidkey.pem") )
        list_threads.append(t)
        t.start()
        # del y.joinIPAddr[rand_gen]
      except Exception as ex:
        print ex

    [t.join() for t in list_threads]

  elif inp == '12':
    break