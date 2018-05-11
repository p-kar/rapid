import commands
import sys

_, op = commands.getstatusoutput("openstack floating ip list -c \"Floating IP Address\" -c \"Fixed IP Address\" -f csv")

nets = op.split('\n')
nets = nets[1:]

publicIPs = []

for item in nets:
  item = item.strip("\",")
  item = item.split("\",\"")
  print item
  if len(item) == 1:
    publicIPs.append(item[0])

print "Done"

_, op = commands.getstatusoutput("openstack server list -c ID -f csv")
nets = op.split('\n')
nets = nets[1:]

nets = [x.strip("\"") for x in nets]

print "Starting assignment"

for x,ip1 in zip(nets, publicIPs):
  try:
    _, op = commands.getstatusoutput("openstack server add floating ip " + x + " " + ip1)
    print op
  except Exception as ex:
    pass
# cmd1 = "python test.py -pub "
# cmd2 = "-pvt "

# for ip1, ip2 in zip(publicIPs, privateIPs):
#   _, _ = commands.getstatusoutput("ssh-keygen -f \"/home/kartik/.ssh/known_hosts\" -R " + ip1)
#   cmd1 = cmd1 + ip1 + " "
#   cmd2 = cmd2 + ip2 + " "

# print cmd1 + cmd2
