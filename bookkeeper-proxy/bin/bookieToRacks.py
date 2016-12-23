#!/usr/bin/env python

import os
import re
import socket
import sys

# The yaml parser isn't supported in python 2.6 that runs on many of our DB nodes;
# So the below code is for parsing a yaml file with the following format:
#
# ipRange: 10.253.212.0/26
# environments:
#  say1:
#    roles:
#      sfstore: 18
# racks:
#   a07-45:
#     subnet: 10.253.212.0/28
#     hosts:
#      - say1shared1-sfstore1-1-prd.eng.sfdc.net
#      - say1shared1-sfstore1-3-prd.eng.sfdc.net
#      - say1shared1-sfstore1-5-prd.eng.sfdc.net
#  a20-45:
#    subnet: 10.253.212.16/28
#    hosts:
#    - say1shared1-sfstore1-2-prd.eng.sfdc.net
#    - say1shared1-sfstore1-4-prd.eng.sfdc.net

try:
    curHostName = socket.gethostname()
    dataCenter = ""
    knownDCs = ["prd", "phx", "dfw"]
    for dc in knownDCs:
        if dc in curHostName:
            dataCenter = dc
            break
    if dataCenter == "":
        print "Couldn't find the yaml file for this DC, exiting"
        sys.exit(1)
except socket.error as why:
    print("Couldn't retrieve the hostname because {0}".format(why))
    sys.exit(1)

baseDir = os.path.realpath(os.path.dirname(os.path.realpath(__file__)))
 
f = open(baseDir + "/../conf/" + dataCenter + '.yaml', 'r')
rackName=""
racksFound=False
hostsFound=False
bookieIPToRack=dict()
for line in f:
    line = line.strip().rstrip()
    if line == "":
        continue
    elif (not racksFound):
        if  (line != "racks:"):
            continue
        else:
            racksFound = True
    else:
        if (rackName == ""):
            rackName = line[0:len(line)-1]
            hostsFound=False
        elif (hostsFound == False):
            if (line != "hosts:"):
                continue
            else:
                hostsFound=True
        elif (line[0] == "-"):
            bookie=line[1:len(line)].strip().rstrip() # ignore the - at the beginning
            resolveHostName=True
            try:
                socket.inet_aton(bookie) # won't throw exception if bookie is already in IP address format
                addr=bookie
                resolveHostName=False # if we already have the host in IP address format
            except socket.error as why:
                resolveHostName=True
            try:
                if resolveHostName:
                    addr = socket.gethostbyname(bookie)
            except socket.error as why:
                print("Failed to convert hostname {0} to IP address because {1}". format(bookie, why))
                sys.exit(1)
            bookieIPToRack[addr] = "/" + rackName
        else:
            rackName = line[0:len(line)-1]
            hostsFound=False

# Now attempt to map the hostname or host IP address to the rackname
if (len(sys.argv) > 1):
    i=0
    for host in sys.argv:
        if (i == 0):
            i += 1 # ignore the script name
            continue
        resolveHostName=False
        try:
            socket.inet_aton(host) # check if it is IP address
        except socket.error as why:
            resolveHostName=True

        try:
            if resolveHostName: ## resolve hostname to IP address, if necessary
                host = socket.gethostbyname(host)
        except socket.error as why:
            print("Couldn't resolve {0} to IP address because {1}".format(host, why))
            sys.exit(1)

        try:
            print bookieIPToRack[host]
        except:
            print("Couldn't find {0} in yaml file".format(host))
            sys.exit(1)

