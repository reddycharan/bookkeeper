#!/usr/bin/env python

import os
import sys
import socket
import fileinput
from sets import Set

'''
   This script will allow us to drop any config override into our container via an environment variable. So long as
   it has a "sf_" prefix, it will be recognized as intended for import, pulled in from environment variables and either
   overlaid over an existing configuration of the same name or written to the file anyway (for those that aren't in 
   the file).
'''

PREFIX = "sf_"

def set_configs(config_file, env_vars):
    added_configs = Set([])
    for line in fileinput.FileInput(config_file, inplace=1):
        # This will be an assignment -- either real or commented out.
        if "=" in line.strip() and not line.strip().startswith("#"):
            elems = line.strip().split("=")
            key = elems[0]
            # Check to see if there's an override provided for this config.
            if key in env_vars:
                # Print the overridden value with no comment.
                print "%s=%s" % (key, env_vars[key])
                # Clear it out & dump remainder at the end.
                added_configs.add(key)
            elif key in common_configs:
                print "%s=%s" % (key, common_configs[key])
                # Clear it out
                added_configs.add(key)
            else:
                print line
        else:
            print line
    fileinput.close()
    # Dump remaining env_vars out
    with open(config_file, 'a') as f:
        for config in env_vars.keys():
            if config not in added_configs:
                f.write("%s=%s\n" % (config, env_vars[config]))

def start_bookie(env_vars):
    os.environ["BOOKIE_LOG_FILE_PATTERN"]=("bookkeeper-bookie-{hostname}-{pattern}.log".
                                           format(hostname=os.environ["HOSTNAME"],
                                            pattern="%d{yyyyMMddHH}"))
    os.environ["BOOKIE_LOG_FORMAT"]="%s.sfstore.bookie" % os.environ["HOSTNAME"]
    os.environ["BOOKIE_LOG_FILE"]="bookkeeper-bookie-%s.log" % os.environ["HOSTNAME"]
    set_configs("/sfs/sfsbuild/conf/bk_server.conf", env_vars)
    os.system("/sfs/sfsbuild/bin/bookkeeper shell initnewcluster")
    os.system("/sfs/sfsbuild/bin/bookkeeper shell bookieformat -d -nonInteractive")
    os.system("/sfs/sfsbuild/bin/bookkeeper bookie")

def start_autorecovery(env_vars):
    os.environ["BOOKIE_LOG_FILE_PATTERN"]=("bookkeeper-autorecovery-{hostname}-{pattern}.log".
                                           format(hostname=os.environ["HOSTNAME"],
                                            pattern="%d{yyyyMMddHH}"))
    os.environ["BOOKIE_LOG_FORMAT"]="%s.sfstore.autorecovery" % os.environ["HOSTNAME"]
    os.environ["BOOKIE_LOG_FILE"]="bookkeeper-autorecovery-%s.log" % os.environ["HOSTNAME"]
    set_configs("/sfs/sfsbuild/conf/bk_replication.conf", env_vars)
    os.system("/sfs/sfsbuild/bin/bookkeeper autorecovery")

if __name__ == '__main__':
    process = sys.argv[1]
    env_vars={}

    # 3 mandatory config changes for sfstore in k8s env: useHostNameAsBookieID, zkLedgersRootPath & zkServers
    # These function as our default config overrides for both autorecovery & bookie. Others shall be provided via
    # env vars.
    common_configs = {}
    common_configs["useHostNameAsBookieID"] = "true"
    common_configs["ledgerDirectories"] = "/sfs/sfsdata/ledger"
    common_configs["journalDirectory"] = "/sfs/sfsdata/journal"

    # Set HOSTNAME
    os.environ["HOSTNAME"] = socket.gethostname()
    
    # Evaluate all environment variables. If any of them has the same name as a config key, set it in the file 
    #and write the file back out. 
    for env in os.environ:
        if env.startswith(PREFIX):
            env_vars[env.replace(PREFIX, "")] = os.getenv(env, "")

    # Check our mandatory keys that must be passed down
    if not env_vars.has_key("zkServers"):
        print "zkServers is missing from provided environment configs. Exiting..."
        sys.exit(1)
    elif not env_vars.has_key("zkLedgersRootPath"):
        print "zkLedgersRootPath is missing from provided environment configs. Exiting..."
        sys.exit(1)
    if process == "bookie":
        start_bookie(env_vars)
        print "Started bookie"
    elif process == "autorecovery":
        start_autorecovery(env_vars)
        print "Started autorecovery"
    elif process == "adminpod":
        set_configs("/sfs/sfsbuild/conf/bk_server.conf", env_vars)
        print "Set configs for admin pod. Sleeping start-up command."
        os.system("sleep infinity")
    else:
        print "Must specify either bookie or autorecovery. Exiting."
        sys.exit(1)

