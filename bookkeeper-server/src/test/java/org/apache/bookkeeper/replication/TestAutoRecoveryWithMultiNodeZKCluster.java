/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.replication;

import static org.apache.zookeeper.client.FourLetterWordMain.send4LetterWord;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperClusterUtil;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.junit.Test;

public class TestAutoRecoveryWithMultiNodeZKCluster extends BookKeeperClusterTestCase {

    public TestAutoRecoveryWithMultiNodeZKCluster() {
        super(3, 7, 120);
    }

    @Test
    public void testAutoRecoveryConnectionDisconnect() throws Exception {
        /*
         * initialize three AutoRecovery instance.
         */
        AutoRecoveryMain main = new AutoRecoveryMain(bsConfs.get(0));

        /*
         * start main1, make sure all the components are started and main1 is
         * the current Auditor
         */
        ZKMetadataClientDriver zkMetadataClientDriver = AutoRecoveryMainTest.startAutoRecoveryMain(main);
        ZooKeeper zkOfARMain = zkMetadataClientDriver.getZk();
        Auditor auditor1 = main.auditorElector.getAuditor();
        BookieSocketAddress currentAuditor = AuditorElector.getCurrentAuditor(bsConfs.get(0), zkOfARMain);
        assertTrue("Current Auditor should be AR1", currentAuditor.equals(Bookie.getBookieAddress(bsConfs.get(0))));
        assertTrue("Auditor of AR1 should be running", auditor1.isRunning());

        
        System.out.println("******** ARMain sid " + Long.toHexString(zkOfARMain.getSessionId()) + " ZKC sid "
                + Long.toHexString(zkc.getSessionId()));

        String zkHexSessionId = Long.toHexString(zkOfARMain.getSessionId());
        int zkServerPeer = -1;
        boolean foundZKServerPeer = false;
        String[] zkServers = zkUtil.getZooKeeperConnectString().split(",");
        for (int i = 0; i < zkServers.length; i++) {
            String zkServer = zkServers[i].trim();
            String zkServerDetails[] = zkServer.split(":");
            String host = zkServerDetails[0];
            int port = Integer.parseInt(zkServerDetails[1]);
            /*
             * 'cons' four letter command, Lists full connection/session details
             * for all clients connected to this server. Includes information on
             * numbers of packets received/sent, session id, operation
             * latencies, last operation performed, etc...
             */
            
            String consOutput = send4LetterWord(host, port, "cons");
            String wchcOutput = send4LetterWord(host, port, "wchc");
            String wchpOutput = send4LetterWord(host, port, "wchp");
            System.out.println("*** peer " + (i+1) );
            System.out.println("  cons");
            System.out.println(consOutput);
            System.out.println("  wchc");
            System.out.println(wchcOutput);
            System.out.println("  wchp");
            System.out.println(wchpOutput);
            if (consOutput.contains(zkHexSessionId)) {
                zkServerPeer = i+1;
                assertFalse("",foundZKServerPeer);
                foundZKServerPeer = true;
            }
        }
        ZooKeeperClusterUtil zkClusterUtil = ((ZooKeeperClusterUtil) zkUtil);
        System.out.println("**************************");
        System.out.println("**** zkServerPeer " + zkServerPeer);
        
        System.out.println("***zkstring " + zkUtil.getZooKeeperConnectString());
        ZooKeeper newzkc = ZooKeeperClient.newBuilder().connectString(zkUtil.getZooKeeperConnectString()).sessionTimeoutMs(10000).build();

        System.out.println("*****arzk " + zkMetadataClientDriver.getZk().getState());
        ((ZooKeeperClusterUtil) zkUtil).quorumUtil.shutdown(zkServerPeer);
        System.out.println("*****arzk " + zkMetadataClientDriver.getZk().getState());
        Thread.sleep(5000);
        System.out.println("*****arzk " + zkMetadataClientDriver.getZk().getState());
        System.out.println("*** zkc getstate "+zkc.getState());
        //Thread.sleep(5000);
        System.out.println("**************************");
        for(int i = 1; i <= zkClusterUtil.numOfZKNodes; i++){
            QuorumPeer peer = zkClusterUtil.quorumUtil.getPeer(i).peer;
            System.out.println(" *** " + i + " alive " + peer.isAlive() + " running " + peer.isRunning());
        }
        /*System.out.println("*** zkc state " + newzkc.getState());
        System.out.println("***zkstring " + zkUtil.getZooKeeperConnectString());
        newzkc = ZooKeeperClient.newBuilder().connectString(zkUtil.getZooKeeperConnectString()).sessionTimeoutMs(10000).build();
        */
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, newzkc);
        System.out.println("*** newzkc getstate "+newzkc.getState());
        System.out.println("----------------");
        urLedgerMgr.markLedgerUnderreplicated(5, "127.0.0.1:5000");
        Thread.sleep(5000);
        System.out.println("----------------");
        System.out.println("**** still ur " + urLedgerMgr.pollLedgerToRereplicate());
        
    }
}
