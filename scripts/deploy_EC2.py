import boto3
import argparse
import time
import paramiko
import sys

class DeployEC2(object):
  
  def __init__(self, number, instanceType, keyName):
    self.number_of_instances = number
    self.instance_type = instanceType
    self.keyName = keyName
    self.client = boto3.client('ec2')
    self.spawned_list = []

  # def _get_userdata(self):
  #   user_data = """#!/bin/bash
  #           """
  #   return user_data

  def _get_security_groups(self):

    security_groups = []
    current_groups = self.client.describe_security_groups(GroupNames=['default'])
    default_security_group_id = current_groups['SecurityGroups'][0]['GroupId']
    security_groups.append(default_security_group_id)
    return security_groups

  def spawn_instances(self):
    
    ami_id = 'ami-4e79ed36'
    # user_data = self._get_userdata()
    security_groups = self._get_security_groups()

    response = self.client.run_instances(
            ImageId=ami_id,
            InstanceType=self.instance_type,
            KeyName=self.keyName,
            MaxCount=self.number_of_instances,
            MinCount=self.number_of_instances,
            Monitoring={'Enabled': False},
            # UserData=user_data,
            SecurityGroupIds=security_groups,
        )
    
    instance_ids = []
    for x in response['Instances']:
      instance_ids.append(x['InstanceId'])
    
    time.sleep(30)
    return instance_ids

  def _get_instance_IPs(self, instance_ids):
    # Use describe_instances call
    IPs = []
    response = self.client.describe_instances(
                InstanceIds=instance_ids,
              )
    for x in response['Reservations']:
      # print x['Instances']
      for y in x['Instances']:
        
        if 'PublicIpAddress' in y.keys():
          IPs.append(y['PublicIpAddress'])
          self.spawned_list.append(y['PublicIpAddress'])  

    return IPs

  def send_Files(self, instance_ids, username, key_filename):
    
    IPs = self._get_instance_IPs(instance_ids)

    for Ip in IPs:
      if Ip in self.spawned_list:
        continue
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      ssh.connect(hostname=Ip, username=username, key_filename=key_filename)

      ftp_client=ssh.open_sftp()
      ftp_client.put('/home/kartik/Development/rapid_tests/EC2_Files/exec.sh','/home/ubuntu/exec.sh')
      ftp_client.close()

      stdin, stdout, stderr = ssh.exec_command("nohup bash /home/ubuntu/exec.sh &")

    print 'done'
    time.sleep(100)

    for Ip in IPs:
      if Ip in self.spawned_list:
        continue
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      ssh.connect(hostname=Ip, username=username, key_filename=key_filename)

      ftp_client=ssh.open_sftp()
      ftp_client.put('/home/kartik/Development/rapid_tests/EC2_Files.zip','/home/ubuntu/EC2_Files.zip')
      ftp_client.close()

      stdin, stdout, stderr = ssh.exec_command('unzip /home/ubuntu/EC2_Files.zip')


# parser = argparse.ArgumentParser()

# parser.add_argument("-n", "--number", help='number of EC2 instances', required=True, type=int)
# parser.add_argument("-t", "--instanceType", help='instance flavor', required=True, type=str)
# parser.add_argument("-k", "--keyName", help='private key name', required=True, type=str)
# opts = parser.parse_args()

x = DeployEC2(2, 't2.micro', 'rapidkey')
instance_ids = x.spawn_instances()
x.send_Files(instance_ids, "ubuntu", "/home/kartik/Development/rapidkey.pem")