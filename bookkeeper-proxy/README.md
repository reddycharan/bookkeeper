bkproxy
=======

The bkproxy code is in two parts 
1. bkproxy-server
2. bkproxy-client

The bkproxy-server is responsible for talking to the bookkeeper, the bkproxy-client is just a test client to read/write data to the server

The bkrpoxy-client project depends on the bkproxy-server

Run the bkproxy-server before the bkproxy-client

Building the code
==================

From command line

1. git clone https://git.soma.salesforce.com/ksubramanian/bkproxy

2. mvn install


Importing to Eclipse

In order for Eclipse to build mvn projects we need to install M2Eclipse. Eclipse plugins are per workspace, so make sure M2Eclipse is installed for the current workspace

1. File -> Import -> Maven -> Existing Maven Project

2. Navigate to bkproxy (you could also import just the bkproxy-server or bkproxy-client)

3. If 'Build Automatically' is checked, Eclipse would just build

Running the server/client
=========================

From command line

1. mvn install 

This will generate two jar files for both the bkproxy-server and bkproxy-client under their respective target directories

2. java -jar bkproxy-server/target/bkproxy-server-1.0-SNAPSHOT-jar-with-dependencies.jar

3. java -jar bkproxy-client/target/bkproxy-client-1.0-SNAPSHOT-jar-with-dependencies.jar 

From Eclipse

To run the bkproxy-server, 

open BKProxyServer.java, right-click anywhere on the file and 'Run As' -> Java Application

To run the bkprox-client, 
 
open BKProxyClient.java right-click anywhere on the file and 'Run As' -> Java Application 
 
