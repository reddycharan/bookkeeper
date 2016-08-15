#!/usr/bin/env python

import os
import sys
import subprocess
import argparse
import shutil
import re
import socket
import smtplib
import time
import ConfigParser
from time import gmtime, strftime, sleep

localOnly=True
frequency=60
diskFullThreshold = 90
baseDir = os.path.realpath(os.path.dirname(os.path.realpath(__file__)))
installDir = ""
sendEmailTo=""

def getTimeStamp():
    return strftime("%a, %d %b %Y %X +0000", gmtime()) + " "

def execCmd(cmd, logFile):
    try:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.wait()
        stdout, stderr = p.communicate()
        logFile.write(getTimeStamp() + "Cmd: \"" + cmd + "\" Stdout: \"" + stdout.rstrip() + "\" Strerr: \"" +
                      stderr.rstrip() + "\"\n")
        success = (True if (stdout != "") else False)
        return success, (stdout if (success) else stderr)
    except OSError as why:
        print("Failed to invoke {0}: {1}".format(cmd, str(why)))
        logFile.write(getTimeStamp() + "Cmd: " + cmd + " Failed to invoke because " + str(why) + "\n")
        return None

def pingHost(host, logFile):
    alertString = ""
    success, result = execCmd("ping -q -c 3 " + host, logFile)
    if not success:
        return result
    match = re.search("3 packets transmitted, 3 received, 0% packet loss", result) or \
            re.search("3 packets transmitted, 3 packets received, 0.0% packet loss", result)
    if match:
        return alertString
    else:
        match = re.search("3 packets transmitted, 0 received, 100% packet loss", result)
        if match:
            alertString += "Host " + host + " is unreachable\n"
        else:
            alertString += "Couldn't determine whether " + host + " is up or not\n"
    if alertString != "":
        logFile.write(getTimeStamp() + alertString + "\n")
    return alertString

def _sfstore_get_bookies(bookieType, logFile):
    errString = ""
    success, output = execCmd(installDir + "/bookkeeper shell listbookies " + bookieType, logFile)
    if not success:
        if re.search("No bookie exists!", output):
            ## if no bookies are ro or rw, the command returns this string in stderr; it's annoying!
            ## so whenever we get failure, we check for the presence of this string, if it is present
            ## we consider it a success
            return True, errString, 0, ""
        else:
            return False, output, 0, ""
    bookies = []
    num_bookies = 0
    for x in output.split('\n'):
        parts = x.split(':')
        if len(parts) == 2:
            try:
                socket.inet_aton(parts[0])
                # if inet_aton passed, ip address part was valid;
                # below we check to see if port # is valid
                if (parts[1].isdigit() and len(parts[1]) <= 5):
                    num_bookies += 1
                    bookies.append(x)
            except socket.error:
                # if parts[0] was not an ip address we come here; ignore it and move to next
                pass
    return True, "", num_bookies, bookies

def findSpaceUsage(host, dirPath, logFile):
    result = ""
    if (localOnly):
        success, result = execCmd("df -h " + dirPath, logFile)
    else:
        success, result = execCmd("ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no " + host + " df -h " + dirPath,
                                  logFile)
    alertString = ""
    if (not success or result == ""):
        alertString = "Couldn't determine space usage on " + host + " for " + dirPath + "\n"
        logFile.write(getTimeStamp() + alertString + "\n")
        return alertString

    #print result
    lines = result.split("\n")
    alertString = "Couldn't determine space usage on " + host + " for " + dirPath + "\n"
    if (len(lines) >=3 ):
        line = lines[len(lines)-2].strip().rstrip() #  line that contains something like "/dev/sdd1       3.7T   61M  3.7T   1% /sfs/sfsbuild"
        vals = line.split()
        if len(vals) > 1:
            usageIdx = len(vals)-2
            match = re.match("(\d+)%", vals[usageIdx])
            if match:
                usedSpacePct = int(match.group(1))
                if (usedSpacePct >= diskFullThreshold):
                    alertString = "Disk Space usage on " + host + " for " + dirPath + " is at " + vals[usageIdx] + "!\n"
                else:
                    alertString = ""
    if alertString != "":
        logFile.write(getTimeStamp() + alertString + "\n")
    return alertString

def isProcRunning(host, proc, logFile):
    if (localOnly):
        success, result = execCmd("ps -aef | grep " + proc + "| grep -v grep", logFile)
    else:
        success, result = execCmd("ssh -o ConnectTimeout=5 StrictHostKeyChecking=no " + host + " ps -aef | grep " +
                                  proc + "| grep -v grep", logFile)
    if (not success or result == ""):
        alertString = "Process " + proc + " doesn't seem to be running on " + host  +"\n"
        logFile.write(getTimeStamp() + alertString)
        return alertString
    return ""

# returns the space used by the entire sfstore cluster by querying the zk cluster
# return values are: errorString, spaceUsed
def getTotalSpaceUsed(logFile):
    errString = ""
    success, output = execCmd(installDir + "/bookkeeper shell listledgers -m > list_ledgers_m.txt", logFile)
    if output != "":
        return "Couldn't retrieve the list of ledgers\n", ""
    success, output = execCmd("grep length list_ledgers_m.txt | awk '{ sum += $2 } END {print sum }'", logFile)
    if not success:
        return "Couldn't get the sum of ledger sizes\n", ""
    return "", output

def convertToHostNameList(bookieList):
    alertString = ""
    hostList=[]
    for b in bookieList:
        # b has the format <ip-address>:<port>
        b=b.strip().rstrip()
        m=re.match("(\d+\.\d+\.\d+.\d+):(\d+)", b)
        if m:
            try:
                bookieHostName=socket.gethostbyaddr(m.group(1))[0]
            except socket.error as why:
                # can't get hostname, just list the ip address
                bookieHostName=m.group(1)
        else:
            # may be hostnames are used as bookie id
            m2=re.match("(\S+):(\d+)", b)
            if m2:
                bookieHostName=m2.group(1)
            else:
                bookieHostName=b
        hostList.append(bookieHostName)
    hostList.sort()
    for h in hostList:
        alertString += h + ", "
    return alertString, hostList

def checkSfStoreStatus(bookies, zkServers, logFile):
    alertString = ""

    # first do the ping test to see what nodes are up
    for host in bookies:
        logFile.write(getTimeStamp() + " working on " + host + "\n")
        result = pingHost(host, logFile)
        logFile.write(getTimeStamp() + " " + host + " Ping result: " + result + "\n")
        if result is not "":
            alertString += result

    # check if any of the bookies are RO; if so list them
    roListAsString=""
    roHostnameList=[]
    success, errString, numBookiesRO, roList = _sfstore_get_bookies("-ro", logFile)
    logFile.write(getTimeStamp() + " listbookies result(readonly): errString: \"" + errString + "\" NumBookies: " +
                  str(numBookiesRO) + "\n")
    if not success:
        alertString += "Couldn't check RO bookie status because: " + errString + "\n"
    elif numBookiesRO > 0:
        alertString += "Found " + str(numBookiesRO) +  " bookies to be in readonly state: "
        roListAsString,  roHostnameList = convertToHostNameList(roList)
        alertString += roListAsString +"\n"

    # now check for RW bookies
    success, errString, numBookiesRW, rwList = _sfstore_get_bookies("-rw", logFile)
    logFile.write(getTimeStamp() + " listbookies result(readwrite): errString: \"" + errString + "\" NumBookies: " +
                  str(numBookiesRW) + "\n")
    if not success:
        alertString += "Couldn't check RW bookie status because: " + errString + "\n"
    else:
        rwListAsString, rwHostnameList = convertToHostNameList(rwList)
        rwListPrinted=False
        if roListAsString != "":
            # if there were RO bookies, then print the RW list for completeness
            alertString += "List of RW bookies: " + rwListAsString + "\n"
            rwListPrinted=True

        combinedList = rwHostnameList + roHostnameList
        bookiesNotInEitherList = list(set(bookies) - set(combinedList))
        if len(bookiesNotInEitherList) > 0:
            if not rwListPrinted:
                # print the RW list, only if we haven't already printed it
                alertString += "List of RW bookies: " + rwListAsString + "\n"
            alertString += "Following bookies are neither RO nor RW: "
            for b in bookiesNotInEitherList:
                alertString += b + ", "
            alertString += "\n"
        newlyAddedBookiesList = list(set(combinedList) - set(bookies))
        if len(newlyAddedBookiesList) > 0:
            if not rwListPrinted:
                alertString += "List of RW bookies: " + rwListAsString + "\n"
            alertString += "Following bookies seem to be newly added to cluster; please update the hosts file : "
            for b in newlyAddedBookiesList:
                alertString += b + ", "
            alertString += "\n"
    # Now check the Zk server status
    for host in zkServers:
        result = pingHost(host, logFile)
        logFile.write(getTimeStamp() + " " + host + " Ping result: " + result + "\n")
        if result is not "":
            alertString += result
    # get the sum of all the ledgers
    errorString, spaceUsed = getTotalSpaceUsed(logFile)
    if errorString != "":
        # if there was an error in retrieving the total space used, send an alert
        alertString += errorString
        return alertString, ""
    return alertString, spaceUsed

def run(doZkChecks, bookies, zkServers, procList, dirPaths, logFile):
    """
       * Validate if all bookies are in RW mode. Send an alert if any bookie is down or in read only mode.
       * Send an alert if storage space usage on sfstore ledger/journal dirs is >90%.
       * Validate if all zookeeper nodes are up and healthy. Send an alert otherwise.
       * Validate Metric Streamer agent is up. Send an alert otherwise.
       * Validate Splunk daemon is up. Send an alert otherwise. Comment this check until Splunk forwarder and indexer are deployed.
    """
    alertString = ""
    success, localHostName = execCmd("hostname", logFile)
    localHostName = localHostName.rstrip()
    for host in bookies:
        if localOnly == True:
            if not host.startswith(localHostName):
                continue
        else:
            logFile.write(getTimeStamp() + " working on " + host + "\n")
            result = pingHost(host, logFile)
            logFile.write(getTimeStamp() + " " + host + " Ping result: " + result + "\n")
            if result is not "":
                alertString += result
                continue
        for d in dirPaths:
            result = findSpaceUsage(host, d, logFile)
            logFile.write(getTimeStamp() + " df -h result for " + d + ": " + host + "\n")
            if result is not "":
                alertString += result
        for proc in procList:
            if proc is "":
                continue
            proc = proc.strip().rstrip()
            result = isProcRunning(host, proc, logFile)
            logFile.write(getTimeStamp() + " proc test for : " + proc + " on " + host + "\n")
            if result is not "":
                alertString += result

    if doZkChecks:
        errorString, spaceUsed = checkSfStoreStatus(bookies, zkServers, logFile)
        if errorString != "":
            alertString += errorString
        if alertString != "" and spaceUsed != "":
            # display the total space used only if there is some other alert identified;
            # else it is not displayed
            size = int(spaceUsed)
            idx = 0;
            suffix = ["B", "KB", "MB", "GB", "TB", "PB"]
            while (size > 1000):
                size = size/1000
                idx += 1
            alertString += "Total space used in the cluster: " + spaceUsed.rstrip()
            if (idx > 0 and idx < len(suffix)):
                alertString += "(" + str(size) + " " + suffix[idx] + ")\n"
            else:
                alertString += "\n"

    print alertString
    if alertString is not "":
        outFile = open(baseDir + "/alerts.txt", "a")
        outFile.write("*************************** ")
        outFile.write(getTimeStamp())
        outFile.write("***************************\n")
        outFile.write(alertString)
        outFile.close()
        if (sendEmailTo):
            sendEmail(sendEmailTo, alertString, logFile)

def runPeriodic(doZkChecks, bookies, zkServers, procList, dirPaths, logFile):
    while (1):
        run(doZkChecks, bookies, zkServers, procList, dirPaths, logFile)
        os.fsync(logFile)
        if (frequency > 0):
            print(getTimeStamp() + " snoozing for " + str(frequency) + " minutes....")
            logFile.write(getTimeStamp() + " snoozing for " + str(frequency) + " minutes....")
            time.sleep(frequency*60)
        else:
            return

def sendEmail(sendTo, alertString, logFile):
    mailFile = baseDir + "/mail.txt"
    f = open(mailFile, "w")
    f.write(alertString)
    f.close()
    execCmd("mail -s \"Alert from sfstore monitor!\" -b rithin.shetty@salesforce.com " + sendTo + "<" + mailFile, logFile)

def do_start(args):
    global installDir, sendEmailTo, frequency, localOnly, diskFullThreshold
    installDir = args.sfstore_install_dir if (args.sfstore_install_dir) else baseDir
    bookieInfoFile = args.sfstore_hosts_info_file if (args.sfstore_hosts_info_file) else baseDir + "/Hosts.txt"
    sendEmailTo = args.send_email_to if (args.send_email_to) else None
    doZkChecks = True if (args.do_zk_checks == "true") else False
    frequency = int(args.frequency) if (args.frequency) else 0
    localOnly = False if (args.local_only == "false") else True
    diskFullThreshold = int(args.disk_full_threshold) if (args.disk_full_threshold) else 90

    logFile = open(baseDir + "/monitor_logs.txt", "a")
    if (os.path.exists(installDir) == False):
        print installDir + " doesn't exist\n"
        sys.exit(1)

    if (os.path.exists(bookieInfoFile) == False):
        print bookieInfoFile + " doesn't exist\n"
        sys.exit(1)

    bookies = []
    zkServers = []
    procList = []
    dirPaths = []
    try:
        config = ConfigParser.ConfigParser()
        config.read(bookieInfoFile)
        bookies = config.get("Info", "Bookies").split(",")
        zkServers = config.get("Info", "ZkServers").split(",")
        procList = config.get("Info", "Processes").split(",")
        dirPaths = config.get("Info", "DirPaths").split(",")
    except ConfigParser.Error as why:
        print("Error while parsing hosts file: " + str(why))
        sys.exit(1)

    print bookies
    print zkServers
    print procList
    print dirPaths
    print localOnly
    logFile.write(getTimeStamp() +  "Bookies: ");
    for b in bookies:
        logFile.write(b + ",")
    logFile.write("\n")
    logFile.write(getTimeStamp() + "zkServers: ");
    for z in zkServers:
        logFile.write(z + ",")
    logFile.write("\n")
    logFile.write(getTimeStamp() + "Processes: ");
    for p in procList:
        logFile.write(p + ",")
    logFile.write("\n")
    logFile.write(getTimeStamp() + "DirPaths: ");
    for d in dirPaths:
        logFile.write(d + ",")
    logFile.write("\n")

    runPeriodic(doZkChecks, bookies, zkServers, procList, dirPaths, logFile)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_start = subparsers.add_parser("start", help="monitor the status of sfstore and related nodes")
    parser_start.add_argument("--sfstore-install-dir",
                              help="Fully qualified path of sfstore install directory; default dir is " +
                                     os.path.realpath(os.path.dirname(os.path.realpath(__file__))))
    parser_start.add_argument("--sfstore-hosts-info-file",
                              help="Fully qualified path of the file containing the comma separated list of bookie "\
                                   "nodes; default is " +
                                    os.path.realpath(os.path.dirname(os.path.realpath(__file__))) + "/Hosts.txt")
    parser_start.add_argument("--send-email-to",
                              help="to which address the email should be sent; default is no email is sent")
    parser_start.add_argument("--do-zk-checks",
                              help="should we check zookeeper to see which nodes are RW and which are RO",
                              choices=["true", "false"])
    parser_start.add_argument("--frequency",
                              help="Duration between running the queries(in minutes); default is 0 which means "\
                                   "run just once and exit")
    parser_start.add_argument("--local-only", help="should the script check only the local machine; default is true",
                              choices=["true", "false"])
    parser_start.add_argument("--disk-full-threshold", help="Above what disk usage should the alert be sent; "\
                                                            "default is 90 percent")
    parser_start.set_defaults(func=do_start)

    cmdline_args = parser.parse_args()
    cmdline_args.func(cmdline_args)
