/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RoundRobinDistributionSchedule.writeSetFromValues;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;
import static org.junit.Assert.assertNotEquals;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementResult;
import org.apache.bookkeeper.client.ZoneawareEnsemblePlacementPolicyImpl.NodePlacementInZone;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.HashedWheelTimer;
import junit.framework.TestCase;

/**
 * Test the zoneaware ensemble placement policy.
 */
public class TestZoneawareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestZoneawareEnsemblePlacementPolicy.class);

    ZoneawareEnsemblePlacementPolicy zepp;
    final List<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
    DistributionSchedule.WriteSet writeSet = DistributionSchedule.NULL_WRITE_SET;
    ClientConfiguration conf = new ClientConfiguration();
    BookieSocketAddress addr1, addr2, addr3, addr4;
    io.netty.util.HashedWheelTimer timer;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(),
                NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        LOG.info("Set up static DNS Resolver.");
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        addr1 = new BookieSocketAddress("127.0.0.2", 3181);
        addr2 = new BookieSocketAddress("127.0.0.3", 3181);
        addr3 = new BookieSocketAddress("127.0.0.4", 3181);
        addr4 = new BookieSocketAddress("127.0.0.5", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), NetworkTopology.DEFAULT_ZONE + "/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), NetworkTopology.DEFAULT_ZONE + "/ud2");
        ensemble.add(addr1);
        ensemble.add(addr2);
        ensemble.add(addr3);
        ensemble.add(addr4);
        writeSet = writeSetFromValues(0, 1, 2, 3);

        timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS, conf.getTimeoutTimerNumTicks());

        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
    }

    @Override
    protected void tearDown() throws Exception {
        zepp.uninitalize();
        super.tearDown();
    }

    static BookiesHealthInfo getBookiesHealthInfo() {
        return getBookiesHealthInfo(new HashMap<>(), new HashMap<>());
    }

    static BookiesHealthInfo getBookiesHealthInfo(Map<BookieSocketAddress, Long> bookieFailureHistory,
            Map<BookieSocketAddress, Long> bookiePendingRequests) {
        return new BookiesHealthInfo() {
            @Override
            public long getBookieFailureHistory(BookieSocketAddress bookieSocketAddress) {
                return bookieFailureHistory.getOrDefault(bookieSocketAddress, -1L);
            }

            @Override
            public long getBookiePendingRequests(BookieSocketAddress bookieSocketAddress) {
                return bookiePendingRequests.getOrDefault(bookieSocketAddress, 0L);
            }
        };
    }

    static void updateMyUpgradeDomain(String zoneAndUD) throws Exception {
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), zoneAndUD);
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostName(), zoneAndUD);
        StaticDNSResolver.addNodeToRack("127.0.0.1", zoneAndUD);
        StaticDNSResolver.addNodeToRack("localhost", zoneAndUD);
    }

    @Test
    public void testNotEnoughRWBookies() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(1);
        newConf.setMinNumZonesPerWriteQuorum(1);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            zepp.newEnsemble(4, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because enough writable nodes are not available");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected to get BKNotEnoughBookiesException
        }

        roAddrs.add(addr4);
        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            zepp.newEnsemble(4, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because enough writable nodes are not available");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected to get BKNotEnoughBookiesException
        }

        rwAddrs.clear();
        roAddrs.add(addr1);
        roAddrs.add(addr2);
        roAddrs.add(addr3);
        roAddrs.add(addr4);
        zepp.onClusterChanged(rwAddrs, roAddrs);
        try {
            zepp.newEnsemble(4, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because enough writable nodes are not available");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected to get BKNotEnoughBookiesException
        }
    }

    @Test
    public void testEnoughRWBookies() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(2);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        PlacementResult<List<BookieSocketAddress>> newEnsemblePlacementResult = zepp.newEnsemble(4, 3, 2, null,
                new HashSet<>());
        Set<BookieSocketAddress> newEnsembleSet = new HashSet<BookieSocketAddress>(
                newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain all 4 rw bookies", newEnsembleSet.containsAll(rwAddrs));

        newEnsemblePlacementResult = zepp.newEnsemble(3, 3, 2, null, new HashSet<>());
        newEnsembleSet = new HashSet<BookieSocketAddress>(newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain 3 rw bookies",
                (newEnsembleSet.size() == 3) && (rwAddrs.containsAll(newEnsembleSet)));
    }

    @Test
    public void testWithDefaultBookies() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone4/ud1");

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        Set<BookieSocketAddress> bookiesInDefaultFaultDomain = new HashSet<BookieSocketAddress>();
        bookiesInDefaultFaultDomain.add(addr5);
        bookiesInDefaultFaultDomain.add(addr6);
        bookiesInDefaultFaultDomain.add(addr7);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);
        rwAddrs.add(addr5);
        rwAddrs.add(addr6);
        rwAddrs.add(addr7);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        for (int i = 0; i < 3; i++) {
            PlacementResult<List<BookieSocketAddress>> newEnsemblePlacementResult = zepp.newEnsemble(4, 4, 2, null,
                    new HashSet<>());
            Set<BookieSocketAddress> newEnsembleSet = new HashSet<BookieSocketAddress>(
                    newEnsemblePlacementResult.getResult());
            assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                    Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));

            newEnsemblePlacementResult = zepp.newEnsemble(3, 3, 2, null, new HashSet<>());
            newEnsembleSet = new HashSet<BookieSocketAddress>(newEnsemblePlacementResult.getResult());
            assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                    Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));
        }
    }
    
    @Test
    public void testMinZonesPerWriteQuorum() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud3");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(3);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> bookiesInDefaultFaultDomain = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);
        rwAddrs.add(addr5);
        rwAddrs.add(addr6);
        rwAddrs.add(addr9);
        rwAddrs.add(addr10);
        roAddrs.add(addr7);
        roAddrs.add(addr8);
        bookiesInDefaultFaultDomain.add(addr9);
        bookiesInDefaultFaultDomain.add(addr10);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        PlacementResult<List<BookieSocketAddress>> newEnsemblePlacementResult;
        try {
            zepp.newEnsemble(4, 3, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because of unable to create ensemble");
        } catch (BKException.BKNotEnoughBookiesException bkne) {
            // expected NotEnoughBookiesException
        }

        newEnsemblePlacementResult = zepp.newEnsemble(6, 3, 2, null, new HashSet<>());
        Set<BookieSocketAddress> newEnsembleSet = new HashSet<BookieSocketAddress>(
                newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain all 6 rw bookies in non-default fault domains",
                rwAddrs.containsAll(newEnsembleSet) && (newEnsembleSet.size() == 6));
        assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));
    }

    @Test
    public void testMinUDsNotAvailable() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud3");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(3);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> bookiesInDefaultFaultDomain = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);
        rwAddrs.add(addr5);
        rwAddrs.add(addr6);
        rwAddrs.add(addr9);
        rwAddrs.add(addr10);
        
        roAddrs.add(addr7);
        roAddrs.add(addr8);

        bookiesInDefaultFaultDomain.add(addr9);
        bookiesInDefaultFaultDomain.add(addr10);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        PlacementResult<List<BookieSocketAddress>> newEnsemblePlacementResult;
        try {
            zepp.newEnsemble(6, 6, 2, null, new HashSet<>());
            fail("newEnsemble is expected to fail because writeQuorum cannt be created with insufficient UDs");
        } catch (BKException.BKNotEnoughBookiesException bkne) {
            // expected NotEnoughBookiesException
        }

        newEnsemblePlacementResult = zepp.newEnsemble(6, 3, 2, null, new HashSet<>());
        Set<BookieSocketAddress> newEnsembleSet = new HashSet<BookieSocketAddress>(
                newEnsemblePlacementResult.getResult());
        assertTrue("New ensemble should contain all 6 rw bookies in non-default fault domains",
                rwAddrs.containsAll(newEnsembleSet) && (newEnsembleSet.size() == 6));
        assertTrue("Bookie from default faultDomain shouldn't be part of ensemble",
                Collections.disjoint(newEnsembleSet, bookiesInDefaultFaultDomain));
    }
    
    
    @Test
    public void testUniqueUds() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);
        BookieSocketAddress addr12 = new BookieSocketAddress("127.0.0.13", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone1/ud3");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), "/zone2/ud3");
        StaticDNSResolver.addNodeToRack(addr12.getHostName(), "/zone2/ud3");

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(2);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);
        rwAddrs.add(addr5);
        rwAddrs.add(addr6);
        rwAddrs.add(addr7);
        rwAddrs.add(addr8);
        rwAddrs.add(addr9);
        rwAddrs.add(addr10);
        rwAddrs.add(addr11);
        rwAddrs.add(addr12);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        PlacementResult<List<BookieSocketAddress>> newEnsemblePlacementResult = zepp.newEnsemble(6, 6, 2, null,
                new HashSet<>());
        List<BookieSocketAddress> newEnsembleList = newEnsemblePlacementResult.getResult();
        Set<BookieSocketAddress> newEnsembleSet = new HashSet<BookieSocketAddress>(newEnsembleList);
        assertTrue("New ensemble should contain 6 rw bookies in non-default fault domains",
                rwAddrs.containsAll(newEnsembleSet) && (newEnsembleSet.size() == 6));
        Set<String> bookiesNetworkLocations = new HashSet<String>();
        for (BookieSocketAddress bookieAddr : newEnsembleSet) {
            bookiesNetworkLocations.add(zepp.resolveNetworkLocation(bookieAddr));
        }
        assertTrue("Bookies should be from different UpgradeDomains if they belong to same zone",
                (bookiesNetworkLocations.size() == 6));
        List<NodePlacementInZone> bookiesNodePlacementList = new ArrayList<NodePlacementInZone>();
        for (BookieSocketAddress bookieAddr : newEnsembleList) {
            bookiesNodePlacementList.add(zepp.getNodePlacementInZone(bookieAddr));
        }
        for (int i = 0; i < 5; i++) {
            assertNotEquals("Alternate bookies should be from different zones", bookiesNodePlacementList.get(i).getZone(),
                    bookiesNodePlacementList.get(i + 1).getZone());
        }
    }

    @Test
    public void testNewBookieUniformDistributionWithMinZoneAndMinUDs() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);
        BookieSocketAddress addr12 = new BookieSocketAddress("127.0.0.13", 3181);
        BookieSocketAddress addr13 = new BookieSocketAddress("127.0.0.14", 3181);
        BookieSocketAddress addr14 = new BookieSocketAddress("127.0.0.15", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr12.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr13.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr14.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);
        rwAddrs.add(addr5);
        rwAddrs.add(addr6);
        rwAddrs.add(addr7);
        rwAddrs.add(addr8);
        rwAddrs.add(addr9);
        rwAddrs.add(addr10);
        rwAddrs.add(addr11);
        rwAddrs.add(addr12);
        rwAddrs.add(addr13);
        rwAddrs.add(addr14);
        
        int minNumZonesPerWriteQuorum = 3;
        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(5);
        newConf.setMinNumZonesPerWriteQuorum(minNumZonesPerWriteQuorum);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        Set<BookieSocketAddress> excludedBookies = new HashSet<BookieSocketAddress>();

        List<BookieSocketAddress> newEnsembleList = zepp.newEnsemble(6, 6, 4, null, excludedBookies).getResult();
        Set<BookieSocketAddress> newEnsembleSet = new HashSet<BookieSocketAddress>(newEnsembleList);
        Set<String> bookiesNetworkLocationsSet = new HashSet<String>();
        List<NodePlacementInZone> bookiesNodePlacementList = new ArrayList<NodePlacementInZone>();
        for (BookieSocketAddress bookieAddr : newEnsembleSet) {
            bookiesNetworkLocationsSet.add(zepp.resolveNetworkLocation(bookieAddr));
        }
        for (BookieSocketAddress bookieAddr : newEnsembleList) {
            bookiesNodePlacementList.add(zepp.getNodePlacementInZone(bookieAddr));
        }
        assertTrue("Bookies should be from different UpgradeDomains if they belong to same zone",
                (bookiesNetworkLocationsSet.size() == 6));
        Set<String> zonesOfFirstNodes = new HashSet<String>();
        for (int i = 0; i < minNumZonesPerWriteQuorum; i++) {
            zonesOfFirstNodes.add(bookiesNodePlacementList.get(i).getZone());
        }
        assertEquals("Num of zones", minNumZonesPerWriteQuorum, zonesOfFirstNodes.size());
        for (int i = 0; i < minNumZonesPerWriteQuorum; i++) {
            assertEquals("Zone", bookiesNodePlacementList.get(i).getZone(),
                    bookiesNodePlacementList.get(i + minNumZonesPerWriteQuorum).getZone());
            assertNotEquals("UpgradeDomain", bookiesNodePlacementList.get(i).getUpgradeDomain(),
                    bookiesNodePlacementList.get(i + minNumZonesPerWriteQuorum).getUpgradeDomain());
        }
    }

    @Test
    public void testReplaceBookie() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);
        BookieSocketAddress addr12 = new BookieSocketAddress("127.0.0.13", 3181);
        BookieSocketAddress addr13 = new BookieSocketAddress("127.0.0.14", 3181);
        BookieSocketAddress addr14 = new BookieSocketAddress("127.0.0.15", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone1/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone2/ud2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr12.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr13.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr14.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(3);
        newConf.setMinNumZonesPerWriteQuorum(3);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);
        rwAddrs.add(addr5);
        rwAddrs.add(addr6);
        rwAddrs.add(addr7);
        rwAddrs.add(addr8);
        rwAddrs.add(addr9);
        rwAddrs.add(addr10);
        rwAddrs.add(addr11);
        rwAddrs.add(addr12);
        rwAddrs.add(addr13);
        rwAddrs.add(addr14);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        List<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
        Set<BookieSocketAddress> excludedBookies = new HashSet<BookieSocketAddress>();
        ensemble.add(addr1);
        ensemble.add(addr5);
        ensemble.add(addr9);
        ensemble.add(addr3);
        ensemble.add(addr7);
        ensemble.add(addr11);
        BookieSocketAddress replacedBookie = zepp.replaceBookie(6, 6, 2, null, ensemble, addr7, excludedBookies)
                .getResult();
        assertEquals("replaced bookie", addr8, replacedBookie);

        excludedBookies.add(addr8);
        replacedBookie = zepp.replaceBookie(6, 3, 2, null, ensemble, addr7, excludedBookies).getResult();
        assertEquals("replaced bookie", addr6, replacedBookie);
        
        excludedBookies.add(addr6);
        try {
            replacedBookie = zepp.replaceBookie(6, 3, 2, null, ensemble, addr7, excludedBookies).getResult();
            fail("Expected BKNotEnoughBookiesException for replaceBookie with added excludedBookies");
        } catch (BKException.BKNotEnoughBookiesException bkne) {
            // expected NotEnoughBookiesException
        }
    }
    
    @Test
    public void testReplaceBookieMinUDs() throws Exception {
        zepp.uninitalize();
        updateMyUpgradeDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        // Update cluster
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.8", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.9", 3181);
        BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.10", 3181);
        BookieSocketAddress addr10 = new BookieSocketAddress("127.0.0.11", 3181);
        BookieSocketAddress addr11 = new BookieSocketAddress("127.0.0.12", 3181);

        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getHostName(), "/zone1/ud1");
        StaticDNSResolver.addNodeToRack(addr2.getHostName(), "/zone2/ud1");
        StaticDNSResolver.addNodeToRack(addr3.getHostName(), "/zone3/ud1");
        StaticDNSResolver.addNodeToRack(addr4.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr5.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr6.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr7.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr8.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr9.getHostName(), "/zone3/ud2");
        StaticDNSResolver.addNodeToRack(addr10.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);
        StaticDNSResolver.addNodeToRack(addr11.getHostName(), NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        ClientConfiguration newConf = (ClientConfiguration) this.conf.clone();
        newConf.setDesiredNumZonesPerWriteQuorum(4);
        newConf.setMinNumZonesPerWriteQuorum(3);
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);
        rwAddrs.add(addr5);
        rwAddrs.add(addr6);
        rwAddrs.add(addr7);
        rwAddrs.add(addr8);
        rwAddrs.add(addr9);
        rwAddrs.add(addr10);
        rwAddrs.add(addr11);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        List<BookieSocketAddress> ensemble = new ArrayList<BookieSocketAddress>();
        Set<BookieSocketAddress> excludedBookies = new HashSet<BookieSocketAddress>();
        ensemble.add(addr1);
        ensemble.add(addr2);
        ensemble.add(addr3);
        ensemble.add(addr4);
        ensemble.add(addr5);
        ensemble.add(addr6);
        BookieSocketAddress replacedBookie = zepp.replaceBookie(6, 6, 2, null, ensemble, addr4, excludedBookies)
                .getResult();
        assertEquals("replaced bookie", "/zone3/ud2", zepp.resolveNetworkLocation(replacedBookie));
    }
}
