#!/usr/bin/env bash

# Run the bookkeeper proxy

# Environment variables:
#   BKPROXY_LOG_DIR           -- location for log files; default: .../logs
#   BKPROXY_LOG_FILE          -- name of log file; default: bkproxy.log
#   BKPROXY_CLASSPATH_PREFIX  -- prepended to CLASSPATH
#   BKPROXY_CLASSPATH_SUFFIX  -- appended to CLASSPATH
#   BKPROXY_HEAP_SIZE         -- Java heap max; default: 1000m
#   BKPROXY_OPTS              -- additional Java options
#   BKPROXY_GC_OPTS           -- Java garbage collection options
#   BKPROXY_NOEXEC            -- run bkproxy in foreground
#
#   JAVA_CMD                  -- Java executable (java); if not set, found from JAVA_HOME
#   JAVA_HOME                 -- location of JDK or JRE
#   MAVEN_HOME                -- Maven location; only used to run in build tree

START_COMMAND="start"
VPOD_START_COMMAND="vpod-start"
CLASSPATH_COMMAND="classpath"

function usage(){
cat << EOF
$0 [commamnd] <options> - run the bookkeeper proxy

${START_COMMAND}:
  $0 ${START_COMMAND} <BKProxyMain options>
    Start the bookkeeper proxy.

  $0 --help || -h
    Show the help for the bookkeeper proxy

${VPOD_START_COMMAND}:
  $0 ${VPOD_START_COMMAND} <BKProxyMain options>
    Start the bookkeeper proxy for vpod environment.

  $0 --help || -h
    Show the help for the bookkeeper proxy
${CLASSPATH_COMMAND}:
  $0 ${CLASSPATH_COMMAND}
    Show the classpath the bookkeeper proxy would run with

Script options:
  All options must be passed BEFORE the command.

  -h       Show this help.
  -d       Enable debugging in the script
EOF
}

if [ $# -eq 0 ]; then
 usage
 exit 1
fi

while getopts ":hd" optname; do
  case ${optname} in
    "h")
			usage
			exit
      ;;
    "d")
      DEBUG=1
			set -x
      ;;
  esac
done

if [ -n "$DEBUG" ]; then
  # remove the script arguments
  shift
fi

if [[ ${1:0:1} = "-" ]]; then
  echo "You started with an option($1) but should have passed a command"
  usage
  exit 1
fi

# resolve links - $0 may be a softlink
######################################
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRGDIR=`dirname "$PRG"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`

# Setup project locations
CONF_DIR="${BASEDIR}/conf"
BKPROXY_HOME=${BASEDIR}

# figure out if we are in a dev env
in_dev_env=false
if [ -d "${BKPROXY_HOME}/target" ]; then
  in_dev_env=true
  BASEDIR=${BASEDIR}/target
fi

# Source environment settings
#############################
# . "${CONF_DIR}/bkproxy-env.sh"

# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "${JAVACMD}" ] ; then
  if [ -n "${JAVA_HOME}"  ] ; then
      JAVACMD="${JAVA_HOME}/bin/java"
  else
    JAVACMD=`which java`
  fi
fi

if [ ! -x "${JAVACMD}" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute ${JAVACMD}"
  exit 1
fi

# Setup classpath
##################
CLASSPATH=${BKPROXY_CLASSPATH_PREFIX}

for FILE in `find ${BASEDIR}/bookkeeper-proxy-*.jar`; do
  CLASSPATH=${CLASSPATH}:${FILE}
done

# Add in the suffix
CLASSPATH=${CLASSPATH}:${BKPROXY_CLASSPATH_SUFFIX}

# Other constants
####################
BKPROXY_LOG_DIR=${BKPROXY_LOG_DIR:-"$BKPROXY_HOME/logs"}
BKPROXY_LOG_FILE=${BKPROXY_LOG_FILE:-"bookkeeper-proxy.log"}
BKPROXY_LOG_LEVEL=${BKPROXY_LOG_LEVEL:-"INFO"}
BKPROXY_LOG_FILE_SIZE=${BKPROXY_LOG_FILE_SIZE:-"100MB"}
BKPROXY_LOG_FILE_COUNT=${BKPROXY_LOG_FILE_COUNT:-"10"}

# Setup the generic java options
#################################

JAVA_OPTS="${BKPROXY_OPTS} ${BKPROXY_GC_OPTS}"
JAVA_OPTS="${JAVA_OPTS} -Dbkproxy.log.dir=${BKPROXY_LOG_DIR}"
JAVA_OPTS="${JAVA_OPTS} -Dbkproxy.log.file=${BKPROXY_LOG_FILE}"
JAVA_OPTS="${JAVA_OPTS} -Dbkproxy.log.level=${BKPROXY_LOG_LEVEL}"
JAVA_OPTS="${JAVA_OPTS} -Dbkproxy.log.filesize=${BKPROXY_LOG_FILE_SIZE}"
JAVA_OPTS="${JAVA_OPTS} -Dbkproxy.log.filecount=${BKPROXY_LOG_FILE_COUNT}"
JAVA_OPTS="${JAVA_OPTS} -Djava.net.preferIPv4Stack=true -Duser.timezone=UTC"
JAVA_OPTS="${JAVA_OPTS} -XX:-MaxFDLimit"
GC_OPTS="-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+DisableExplicitGC -verbose:gc "
JAVA_OPTS="${JAVA_OPTS} ${GC_OPTS}"

JAVA_VPOD_OPTS=" -XX:PermSize=1024m -XX:MaxPermSize=1024m -XX:ParGCCardsPerStrideChunk=32768 -XX:InitialCodeCacheSize=128m -XX:ReservedCodeCacheSize=128m -Xss296k -XX:NewSize=4G -XX:MaxNewSize=4G -XX:MaxTenuringThreshold=2 -XX:+UnlockDiagnosticVMOptions -XX:+UseCMSInitiatingOccupancyOnly -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSScavengeBeforeRemark -XX:CMSInitiatingPermOccupancyFraction=80 -XX:+CMSParallelInitialMarkEnabled -XX:+CMSEdenChunksRecordAlways -XX:+ParallelRefProcEnabled -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:PrintFLSStatistics=1 -XX:StackShadowPages=20 -XX:+UseTLAB -XX:+UseCompressedOops -XX:-UseBiasedLocking -XX:+PrintStringTableStatistics -XX:StringTableSize=1000003" #-XX:+UseLinuxPosixThreadCPUClocks

#####################
# Parse arguments
###################
# Determine command
case $1 in
   ${START_COMMAND})
     JAVA_HEAP_MAX=${BKPROXY_HEAPSIZE:-"1000"}
     JAVA_HEAP_MAX="-Xmx${JAVA_HEAP_MAX}m"
     MAIN_CLASS="org.apache.bookkeeper.BKProxyMain"
     ;;
   ${VPOD_START_COMMAND})
     JAVA_HEAP_MAX=${BKPROXY_HEAPSIZE:-"6000"}
     JAVA_HEAP_MAX="-Xms${JAVA_HEAP_MAX}m -Xmx${JAVA_HEAP_MAX}m"
     JAVA_OPTS="${JAVA_OPTS} ${JAVA_VPOD_OPTS}"
     MAIN_CLASS="org.apache.bookkeeper.BKProxyMain"
     ;;
   ${CLASSPATH_COMMAND})
     echo "Using classpath: ${CLASSPATH}"
     exit 0
     ;;
esac
# remove the first argument so we can easily pass the remaining args
shift

#################
# Run the program
#################

# Exec unless NOEXEC is set.
if [ "${BKPROXY_NOEXEC}" != "" ]; then
 "${JAVACMD}" -XX:OnOutOfMemoryError="kill -9 %p" \
    ${JAVA_HEAP_MAX} \
    ${JAVA_OPTS} \
    -classpath "${CLASSPATH}" \
    -Dapp.pid="$$" \
    ${MAIN_CLASS} \
    "$@"
else
 exec "${JAVACMD}" -XX:OnOutOfMemoryError="kill -9 %p" \
    ${JAVA_HEAP_MAX} \
    ${JAVA_OPTS} \
    -classpath "${CLASSPATH}" \
    -Dapp.pid="$$" \
    ${MAIN_CLASS} \
    "$@"
fi
