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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import junit.framework.Assert;
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

        timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());

        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
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

        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
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
        
        zepp = new ZoneawareEnsemblePlacementPolicy();
        zepp.initialize(conf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL, NullStatsLogger.INSTANCE);
        zepp.withDefaultFaultDomain(NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN);

        Set<BookieSocketAddress> rwAddrs = new HashSet<BookieSocketAddress>();
        Set<BookieSocketAddress> roAddrs = new HashSet<BookieSocketAddress>();
        rwAddrs.add(addr1);
        rwAddrs.add(addr2);
        rwAddrs.add(addr3);
        rwAddrs.add(addr4);

        zepp.onClusterChanged(rwAddrs, roAddrs);
        zepp.newEnsemble(4, 3, 2, null, new HashSet<>());

    }    
}
