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

GC_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ResizeTLAB -XX:-ResizePLAB -XX:MetaspaceSize=128m -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+HeapDumpOnOutOfMemoryError"
GC_OPTS="$GC_OPTS -XX:G1HeapRegionSize=8M -XX:ParallelGCThreads=6 -XX:+ParallelRefProcEnabled -XX:StackShadowPages=20 -XX:+UseCompressedOops -XX:+DisableExplicitGC -XX:StringTableSize=1000003" 
GC_OPTS="$GC_OPTS -XX:InitiatingHeapOccupancyPercent=75"
GC_OPTS="$GC_OPTS -verbose:gc -XX:+PrintHeapAtGC -XX:+PrintPromotionFailure -XX:+PrintClassHistogramBeforeFullGC -XX:+PrintClassHistogramAfterFullGC -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:PrintFLSStatistics=1 -XX:+PrintStringTableStatistics"
MEM_OPTS="-Xms6G -Xmx6G"

# Extra options to be passed to the jvm
# BOOKIE_EXTRA_OPTS=
BOOKIE_EXTRA_OPTS="$MEM_OPTS $GC_OPTS -Duser.timezone=UTC -XX:+UnlockDiagnosticVMOptions -XX:-OmitStackTraceInFastThrow "

# Add extra paths to the bookkeeper classpath
# BOOKIE_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#BOOKIE_PID_DIR=

#Wait time before forcefully kill the Bookie server instance, if the stop is not successful
#BOOKIE_STOP_TIMEOUT=
