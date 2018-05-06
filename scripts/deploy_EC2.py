import boto3
import argparse
import time
import paramiko
import sys

class DeployEC2(object):
  
  def __init__(self, number=0, instanceType='t2.micro', keyName='default'):
    self.number_of_instances = number
    self.instance_type = instanceType
    self.keyName = keyName
    self.client = boto3.client('ec2')
    self.spawned_list = []

  def _get_userdata(self):
    user_data = """#!/bin/bash
            """
    return user_data

  def _get_security_groups(self):

    security_groups = []
    current_groups = self.client.describe_security_groups(GroupNames=['default'])
    default_security_group_id = current_groups['SecurityGroups'][0]['GroupId']
    security_groups.append(default_security_group_id)
    return security_groups

  def spawn_instances(self, ami_id = 'ami-aa94fdd2'):

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
            TagSpecifications=[
              {
                  'ResourceType': 'instance',
                  'Tags': [
                      {
                          'Key': 'rapid',
                          'Value': 'rapid'
                      },
                  ]
              },
            ],     
        )
    
    instance_ids = []
    for x in response['Instances']:
      instance_ids.append(x['InstanceId'])
    
    time.sleep(self.number_of_instances * 2)
    
    return instance_ids

  def _get_instance_IPs(self, instance_ids):
    # Use describe_instances call
    publicIPs = []
    privateIPs = []
    
    response = self.client.describe_instances(
                InstanceIds=instance_ids,
                Filters=[
                    {
                        'Name': 'tag-key',
                        'Values': [
                            'rapid',
                        ]
                    },
                ],
              )
    for x in response['Reservations']:
      # print x['Instances']
      for y in x['Instances']:
        
        if 'PublicIpAddress' in y.keys():
          publicIPs.append(y['PublicIpAddress'])
          privateIPs.append(y['PrivateIpAddress'])
          

    return publicIPs, privateIPs

  def _get_instance_IDs(self):
    instance_ids = []

    response = self.client.describe_instances(
                InstanceIds=instance_ids,
                Filters=[
                    {
                        'Name': 'tag-key',
                        'Values': [
                            'rapid',
                        ]
                    },
                ],
              )

    for x in response['Reservations']:
      # print x['Instances']
      for y in x['Instances']:
        
        if 'PublicIpAddress' in y.keys():
           instance_ids.append(y['InstanceId'])

    # print instance_ids
    return instance_ids

  def terminate_instances(self, instance_ids):
    response = self.client.terminate_instances(
                  InstanceIds=instance_ids,
              )
    return response

  def stop_instances(self, instance_ids):
    response = self.client.stop_instances(
                  InstanceIds=instance_ids,
              )
    return response

  def start_instances(self, instance_ids):
    response = self.client.start_instances(
                  InstanceIds=instance_ids,
              )
    return response

# parser = argparse.ArgumentParser()

# parser.add_argument("-n", "--number", help='number of EC2 instances', required=True, type=int)
# parser.add_argument("-t", "--instanceType", help='instance flavor', required=True, type=str)
# parser.add_argument("-k", "--keyName", help='private key name', required=True, type=str)
# opts = parser.parse_args()

# x = DeployEC2(2, 't2.micro', 'rapidkey')
# instance_ids = x.spawn_instances()
# x.send_Files(instance_ids, "ec2-user", "/home/kartik/Development/rapidkey.pem")