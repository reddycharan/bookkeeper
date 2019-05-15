/**
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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_JOINED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_LEFT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER;
import static org.apache.bookkeeper.client.BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import io.netty.util.HashedWheelTimer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.Configurable;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.NetworkTopologyImpl;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.net.StabilizeNetworkTopology;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple zoneaware ensemble placement policy.
 */
public class ZoneawareEnsemblePlacementPolicyImpl extends TopologyAwareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(ZoneawareEnsemblePlacementPolicyImpl.class);
    public static final String REPP_DNS_RESOLVER_CLASS = "reppDnsResolverClass";
    private String defaultFaultDomain = NetworkTopology.DEFAULT_ZONE_UD;
    protected StatsLogger statsLogger = null;
    // Use a loading cache so slow bookies are expired. Use entryId as values.
    protected Cache<BookieSocketAddress, Long> slowBookies;
    protected BookieNode localNode = null;
    protected boolean reorderReadsRandom = false;
    protected int stabilizePeriodSeconds = 0;
    protected int reorderThresholdPendingRequests = 0;
    protected int maxWeightMultiple;
    protected HashedWheelTimer timer;

    @StatsDoc(
            name = FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER,
            help = "Counter for number of times DNSResolverDecorator failed to resolve Network Location"
    )
    protected Counter failedToResolveNetworkLocationCounter = null;
    @StatsDoc(
            name = NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN,
            help = "Gauge for the number of writable Bookies in default fault domain"
    )
    protected Gauge<Integer> numWritableBookiesInDefaultFaultDomain;

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
            Optional<DNSToSwitchMapping> optionalDnsResolver, HashedWheelTimer timer,
            FeatureProvider featureProvider, StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        this.timer = timer;
        this.bookiesJoinedCounter = statsLogger.getOpStatsLogger(BOOKIES_JOINED);
        this.bookiesLeftCounter = statsLogger.getOpStatsLogger(BOOKIES_LEFT);
        this.failedToResolveNetworkLocationCounter = statsLogger.getCounter(FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER);
        this.numWritableBookiesInDefaultFaultDomain = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                rwLock.readLock().lock();
                try {
                    return topology.countNumOfAvailableNodes(getDefaultFaultDomain(), Collections.emptySet());
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        };
        this.statsLogger.registerGauge(NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN,
                numWritableBookiesInDefaultFaultDomain);
        this.reorderThresholdPendingRequests = conf.getReorderThresholdPendingRequests();
        this.isWeighted = conf.getDiskWeightBasedPlacementEnabled();
        if (this.isWeighted) {
            this.maxWeightMultiple = conf.getBookieMaxWeightMultipleForWeightBasedPlacement();
            this.weightedSelection = new WeightedRandomSelectionImpl<BookieNode>(this.maxWeightMultiple);
            LOG.info("Weight based placement with max multiple of " + this.maxWeightMultiple);
        } else {
            LOG.info("Not weighted");
        }
        DNSToSwitchMapping actualDNSResolver;
        if (optionalDnsResolver.isPresent()) {
            actualDNSResolver = optionalDnsResolver.get();
        } else {
            String dnsResolverName = conf.getString(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
            actualDNSResolver = ReflectionUtils.newInstance(dnsResolverName, DNSToSwitchMapping.class);
            if (actualDNSResolver instanceof Configurable) {
                ((Configurable) actualDNSResolver).setConf(conf);
            }
        }

        this.dnsResolver = new DNSResolverDecorator(actualDNSResolver, () -> this.getDefaultFaultDomain(),
                failedToResolveNetworkLocationCounter);
        this.stabilizePeriodSeconds = conf.getNetworkTopologyStabilizePeriodSeconds();
        // create the network topology
        if (stabilizePeriodSeconds > 0) {
            this.topology = new StabilizeNetworkTopology(timer, stabilizePeriodSeconds);
        } else {
            this.topology = new NetworkTopologyImpl();
        }
        try {
            localNode = createBookieNode(new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0));
        } catch (UnknownHostException e) {
            LOG.error("Failed to get local host address : ", e);
            throw new RuntimeException(e);
        }
        LOG.info("Initialize zoneaware ensemble placement policy @ {} @ {} : {}.", localNode,
                localNode.getNetworkLocation(), dnsResolver.getClass().getName());

        slowBookies = CacheBuilder.newBuilder()
                .expireAfterWrite(conf.getBookieFailureHistoryExpirationMSec(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<BookieSocketAddress, Long>() {
                    @Override
                    public Long load(BookieSocketAddress key) throws Exception {
                        return -1L;
                    }
                });
        return this;
    }

    public ZoneawareEnsemblePlacementPolicyImpl withDefaultFaultDomain(String defaultFaultDomain) {
        checkNotNull(defaultFaultDomain, "Default rack cannot be null");

        this.defaultFaultDomain = defaultFaultDomain;
        return this;
    }

    public String getDefaultFaultDomain() {
        return defaultFaultDomain;
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Set<BookieSocketAddress> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> parentEnsemble,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> parentPredicate)
            throws BKNotEnoughBookiesException {
        return null;
    }

    @Override
    public BookieNode selectFromNetworkLocation(String networkLoc, Set<Node> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> predicate,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom) throws BKNotEnoughBookiesException {
        throw new UnsupportedOperationException(
                "selectFromNetworkLocation is not supported for ZoneawareEnsemblePlacementPolicyImpl");
    }

    @Override
    public BookieNode selectFromNetworkLocation(Set<String> excludeRacks, Set<Node> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> predicate,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom) throws BKNotEnoughBookiesException {
        throw new UnsupportedOperationException(
                "selectFromNetworkLocation is not supported for ZoneawareEnsemblePlacementPolicyImpl");
    }

    @Override
    public BookieNode selectFromNetworkLocation(String networkLoc, Set<String> excludeRacks, Set<Node> excludeBookies,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Predicate<BookieNode> predicate,
            org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy.Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom) throws BKNotEnoughBookiesException {
        throw new UnsupportedOperationException(
                "selectFromNetworkLocation is not supported for ZoneawareEnsemblePlacementPolicyImpl");
    }

    @Override
    public void uninitalize() {
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PlacementResult<BookieSocketAddress> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void registerSlowBookie(BookieSocketAddress bookieSocketAddress, long entryId) {
     // TODO Auto-generated method stub
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(
            List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        DistributionSchedule.WriteSet retList = reorderReadSequence(
                ensemble, bookiesHealthInfo, writeSet);
        retList.addMissingIndices(ensemble.size());
        return retList;
    }
}
