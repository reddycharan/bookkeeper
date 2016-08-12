#!/bin/sh
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting bookkeeper

# Configuration file of settings used in bookie server
# BOOKIE_CONF=

# Log4j configuration file
# BOOKIE_LOG_CONF=

# Logs location
# BOOKIE_LOG_DIR=

# Extra options to be passed to the jvm
# In our VPOD setups, we presume 32GB of RAM
# BOOKIE_EXTRA_OPTS=
BOOKIE_EXTRA_OPTS="-Xms16G -Xmx16G -Xmn8G -Duser.timezone=UTC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSScavengeBeforeRemark -XX:+CMSParallelInitialMarkEnabled -XX:+ParallelRefProcEnabled -XX:+UnlockDiagnosticVMOptions -XX:+CMSEdenChunksRecordAlways -XX:ParGCCardsPerStrideChunk=4096 -XX:StringTableSize=1000003 -verbose:gc -XX:+PrintHeapAtGC -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:-OmitStackTraceInFastThrow -XX:PrintFLSStatistics=1 -XX:+PrintStringTableStatistics "

# Add extra paths to the bookkeeper classpath
# BOOKIE_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#BOOKIE_PID_DIR=

#Wait time before forcefully kill the Bookie server instance, if the stop is not successful
#BOOKIE_STOP_TIMEOUT=

#Entry formatter class to format entries.
#ENTRY_FORMATTER_CLASS=

#LedgerId formatter class to format ledgerids.
LEDGERID_FORMATTER_CLASS=org.apache.bookkeeper.util.LedgerIdFormatter\$UUIDLedgerIdFormatter

#Port number through which you want to enable JMX RMI connections. Be sure to specify an unused port number. In addition to publishing an RMI connector for local access, setting this property publishes an additional RMI connector in a private read-only registry at the specified port using a well known name, "jmxrmi". For more info. - https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html
JMX_PORT_NUM=9010
