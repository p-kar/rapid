import commands

_, op = commands.getstatusoutput("openstack server list -c Networks -f csv")

nets = op.split('\n')
nets = nets[1:]

publicIPs = []
privateIPs = []

for item in nets:
  try:
    _, ips = item.split('=')
    ip2, ip1 = ips.split(',')
    ip1 = ip1.strip(" \"")
    publicIPs.append(ip1)
    privateIPs.append(ip2)
  except Exception as ex:
    print ex
  # print ip1, ip2

cmd1 = "python test.py -pub "
cmd2 = "-pvt "

for ip1, ip2 in zip(publicIPs, privateIPs):
  _, _ = commands.getstatusoutput("ssh-keygen -f \"/home/kartik/.ssh/known_hosts\" -R " + ip1)
  cmd1 = cmd1 + ip1 + " "
  cmd2 = cmd2 + ip2 + " "

print cmd1 + cmd2