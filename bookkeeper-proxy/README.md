bookkeeper-proxy
================

The bookkeeper-proxy is responsible for accepting requests and communicating 
with bookkeeper through the client APIs.

Building the code
==================

From command line

1. git clone https://git.soma.salesforce.com/SFStoreage/bookkeeper.git

2. cd bookkeeper

3. git checkout -b sfstore origin/sfstore

4. make sure your JAVA_HOME is set

5. mvn clean; mvn install -DskipTests=true


Importing to Eclipse

In order for Eclipse to build mvn projects we need to install M2Eclipse. Eclipse plugins are per workspace, so make sure M2Eclipse is installed for the current workspace

1. File -> Import -> Maven -> Existing Maven Project

2. Navigate to bookkeeper-proxy 

3. If 'Build Automatically' is checked, Eclipse would just build

Running the proxy
=========================

From command line, run

	java -jar bookkeeper-proxy/target/bookkeeper-proxy-4.4.0-SNAPSHOT-jar-with-dependencies.jar

From Eclipse

To run the bookkeeper proxy,

open BKProxyMain.java, right-click anywhere on the file and 'Run As' -> Java Application
