#/bin/sh

#Usage: ./setProxyLogLevel.sh (FATAL|ERROR|WARN|INFO|DEBUG|TRACE)

LEVEL=$1
case $LEVEL in
	FATAL|ERROR|WARN|INFO|DEBUG|TRACE)
		echo "Setting debug level to $LEVEL"
		sed -i.backup "s#level=\".*\"\s#level=\"$LEVEL\"\ #" ../conf/log4j2.server.xml
		if [ "$?" == 0 ]; then
			rm ../conf/log4j2.server.xml.backup
			echo "Updated logging level to $LEVEL. Please wait 15 seconds for new level to reflect in logs."
		else
			#Error occurred. Replace contents with original
			echo "An error occurred. Reverting to previous version."
			mv ../conf/log4j2.server.xml.backup ../conf/log4j2.server.xml
		fi;;
	*)
		echo "Specified level must be FATAL, ERROR, WARN, INFO, DEBUG or TRACE." && exit 1;;
esac