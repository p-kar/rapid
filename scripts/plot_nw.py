import os
import sys
import numpy as np
import matplotlib.pyplot as plt
# import glob
import fnmatch

# files = list(glob.iglob(os.path.join("/home/kartik/Development/rapid_tests/logs/", '*/' + ".log"), recursive=True))

# print(files)

matches = []
for root, dirnames, filenames in os.walk('Bootstrap/RM_Net/'):
    for filename in fnmatch.filter(filenames, '*.ERR'):
        matches.append(os.path.join(root, filename))


readings = {}
for filename in matches:
  f = open(filename, "r")
  curTX = 0.0
  curRX = 0.0
  idx = 0  
  flag = False
  for line in f:

    if "eno1" in line:
      flag = True
      idx += 1
    elif "lo" in line or "eno2" in line or "eno3" in line or "eno4" in line:
      flag = False

    if flag is True:
      if "RX" in line:
        try:   
          vals = line.split(':')
          # maxval, _ = vals[1].split()
          # avgval, _ = vals[2].split()
          # nwMaxRX.append(float(maxval)/1000.0)
          # nwAvgRX.append(float(avgval)/1000.0)
          # curRX.append(float(vals[3])/1000.0)
          curRX = float(vals[3])/1000.0
        except Exception as ex:
          pass

      elif "TX" in line:
        try:   
          vals = line.split(':')
          # maxval, _ = vals[1].split()
          # avgval, _ = vals[2].split()
          # nwMaxTX.append(float(maxval)/1000.0)
          # nwAvgTX.append(float(avgval)/1000.0)
          # curTX.append(float(vals[3])/1000.0)
          curTX = float(vals[3])/1000.0
        except Exception as ex:
          pass

      if idx not in readings.keys():
        readings[idx] = []

      readings[idx].append([curRX, curTX])
      

  f.close()

# MaxRX = []
# AvgRX = []
# _99ptRX = []

# MaxTX = []
# AvgTX = []
# _99ptTX = []
overall_avg_rx = 0.0
overall_avg_tx = 0.0
overall_max_rx = 0.0
overall_max_tx = 0.0
overall_99pt_rx = 0.0
overall_99pt_tx = 0.0
for i in xrange(44):
  valRX = []
  valTX = []
  avgRX = 0.0
  avgTX = 0.0
  for key, val in readings.iteritems():
     try:    
       valRX.append(val[i][0])
       valTX.append(val[i][1])
       avgRX += val[i][0]
       avgTX += val[i][1]
     except Exception as ex:
      break
  valRX = sorted(valRX)
  valTX = sorted(valTX)
  avgRX = avgRX / len(readings.keys())
  avgTX = avgTX / len(readings.keys())
  maxRX = valRX[-1]
  maxTX = valTX[-1]
  _99ptRX = valRX[-2]
  _99ptTX = valTX[-2]
  overall_max_rx = max(overall_max_rx, maxRX)
  overall_max_tx = max(overall_max_tx, maxTX)
  overall_99pt_rx = max(overall_99pt_rx, _99ptRX)
  overall_99pt_tx = max(overall_99pt_tx, _99ptTX)
  overall_avg_rx = max(overall_avg_rx, avgRX)
  overall_avg_tx = max(overall_avg_tx, avgTX)
  # print maxRX, _99ptRX, avgRX, maxTX, _99ptTX, avgTX

print overall_max_rx/23.0, overall_max_tx/23.0, overall_99pt_rx/23.0, overall_99pt_tx/23.0, overall_avg_rx/23.0, overall_avg_tx/23.0
    # maxRx = -1
    # avgRx = 0
    # maxTx = -1
    # avgTx = 0
    # valRX = []
    # valTX = []
    # for x in val:
    #   valRX.append(x[0])
    #   valTX.append(x[1])
    #   maxRx = max(maxRx, x[0])
    #   avgRx += x[0]
    #   maxTx = max(maxTx, x[1])
    #   avgTx += x[1]

    # valRX = sorted(valRX)
    # valTX = sorted(valTX)
    # avgRx = avgRx / len(val)
    # avgTx = avgTx / len(val)
    # # MaxRX.append(maxRx)
    # # AvgRX.append(avgRx)
    # # _99ptRX.append(valRX[-2])
    # # MaxTX.append(maxTx)
    # # AvgTX.append(avgTx)
    # # _99ptTX.append(valTX[-2])
    # print "TS:", str(idx),
    # print maxRx, valRX[-2], avgRx, ";", maxTx, valTX[-2], avgTx




# idxList = [x for x in range(1,len(readings.keys()) + 1)]

# plt.figure(1)
# plt.plot(idxList, MaxRX)
# plt.plot(idxList, AvgRX)
# plt.xlabel("timestep")
# plt.ylabel("KB/s (RX)")

# plt.figure(2)
# plt.plot(idxList, MaxTX)
# plt.plot(idxList, AvgTX)
# plt.xlabel("timestep")
# plt.ylabel("KB/s (TX)")

# plt.show()
