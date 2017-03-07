#! /bin/python

import sys
import os

"""
    Usage: nagiosGenerator.py [environment] [templateFile] [environmentFile]
    Output: "nagios-[environmentType].txt" with all outputs for that given environment. 
"""

targetFile = ""
templateFile = ""
hostFile = ""
templateText=""

def generateFile():
    print "Reading from template file: {}".format(templateFile)
    print "Host file: {}".format(hostFile)
    outputFileRef = open(targetFile, 'w+')
    templateFileRef = open(templateFile, 'r+')
    templateText = templateFileRef.read()
    hostCount = 0
    with open(hostFile, 'r+') as hostFileRef:
        for currentHost in hostFileRef:
            if ".ops.sfdc.net" in currentHost:
                currentHost = currentHost[:(len(".ops.sfdc.net") + 1) * -1]
            outputFileRef.write(templateText.replace("{{host}}", currentHost) + "\n")
            hostCount += 1
    templateFileRef.close()
    outputFileRef.close()
    print "Wrote configs for {} hosts to {}".format(hostCount, targetFile)
    
    
    


if __name__ == "__main__":
    if (len(sys.argv) < 2):
        print "Need to provide target type (e.g., phx, dfw, prd, etc.). Exiting."
        sys.exit(1)
    environment=sys.argv[1]
    templateFile=sys.argv[2]
    hostFile=sys.argv[3]
    targetFile="nagios-{}.cfg".format(environment)
    generateFile()
