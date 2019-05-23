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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.net.StabilizeNetworkTopology;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple zoneaware ensemble placement policy.
 */
public class ZoneawareEnsemblePlacementPolicyImpl extends TopologyAwareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(ZoneawareEnsemblePlacementPolicyImpl.class);
    public static final String REPP_DNS_RESOLVER_CLASS = "reppDnsResolverClass";
    public static final String UNKNOWN_ZONE = "UnknownZone";
    private String defaultFaultDomain = NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN;
    protected StatsLogger statsLogger = null;
    // Use a loading cache so slow bookies are expired. Use entryId as values.
    protected Cache<BookieSocketAddress, Long> slowBookies;
    protected BookieNode localNode = null;
    protected String myZone = null;
    protected boolean reorderReadsRandom = false;
    protected int stabilizePeriodSeconds = 0;
    protected int reorderThresholdPendingRequests = 0;
    protected int maxWeightMultiple;
    protected int minNumZonesPerWriteQuorum;
    protected int desiredNumZonesPerWriteQuorum;
    protected HashedWheelTimer timer;
    protected final ConcurrentMap<BookieSocketAddress, NodePlacementInZone> address2NodePlacement;

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

    /**
     * Zone and UpgradeDomain pair of a node.
     */
    public static class NodePlacementInZone {
        private final String zone;
        private final String upgradeDomain;
        private final String repString;

        public NodePlacementInZone(String zone, String upgradeDomain) {
            this.zone = zone;
            this.upgradeDomain = upgradeDomain;
            repString = zone + NodeBase.PATH_SEPARATOR_STR + upgradeDomain;
        }

        public String getZone() {
            return zone;
        }

        public String getUpgradeDomain() {
            return upgradeDomain;
        }

        @Override
        public int hashCode() {
            return repString.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return ((obj instanceof NodePlacementInZone) && repString.equals(((NodePlacementInZone) obj).repString));
        }
    }

    public static final NodePlacementInZone UNKNOWN_NODEPLACEMENT = new NodePlacementInZone(
            NetworkTopology.DEFAULT_ZONE.replace(NodeBase.PATH_SEPARATOR_STR, ""),
            NetworkTopology.DEFAULT_UPGRADEDOMAIN.replace(NodeBase.PATH_SEPARATOR_STR, ""));

    ZoneawareEnsemblePlacementPolicyImpl() {
        super();
        address2NodePlacement = new ConcurrentHashMap<BookieSocketAddress, NodePlacementInZone>();
    }

    protected NodePlacementInZone getNodePlacementInZone(BookieSocketAddress addr) {
        NodePlacementInZone nodePlacement = address2NodePlacement.get(addr);
        if (null == nodePlacement) {
            String networkLocation = resolveNetworkLocation(addr);
            if (NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN.equals(networkLocation)) {
                nodePlacement = UNKNOWN_NODEPLACEMENT;
            } else {
                String[] parts = StringUtils.split(NodeBase.normalize(networkLocation), NodeBase.PATH_SEPARATOR);
                if (parts.length <= 1) {
                    nodePlacement = UNKNOWN_NODEPLACEMENT;
                } else {
                    nodePlacement = new NodePlacementInZone(parts[0], parts[1]);
                }
            }
            address2NodePlacement.putIfAbsent(addr, nodePlacement);
        }
        return nodePlacement;
    }

    protected NodePlacementInZone getLocalNodePlacementInZone(BookieNode node) {
        if (null == node || null == node.getAddr()) {
            return UNKNOWN_NODEPLACEMENT;
        }
        return getNodePlacementInZone(node.getAddr());
    }

    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf,
            Optional<DNSToSwitchMapping> optionalDnsResolver, HashedWheelTimer timer, FeatureProvider featureProvider,
            StatsLogger statsLogger) {
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
        this.minNumZonesPerWriteQuorum = conf.getMinNumZonesPerWriteQuorum();
        this.desiredNumZonesPerWriteQuorum = conf.getDesiredNumZonesPerWriteQuorum();
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
            myZone = getLocalNodePlacementInZone(localNode).getZone();
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
        int bookieToReplaceIndex = currentEnsemble.indexOf(bookieToReplace);
        int desiredNumZonesPerWriteQuorumForThisEnsemble = (writeQuorumSize < desiredNumZonesPerWriteQuorum)
                ? writeQuorumSize : desiredNumZonesPerWriteQuorum;
        rwLock.readLock().lock();
        try {
            Set<String> zonesToExclude = getZonesOfNeighboringNodesInEnsemble(currentEnsemble, bookieToReplaceIndex,
                    (desiredNumZonesPerWriteQuorumForThisEnsemble - 1));
            Set<BookieSocketAddress> bookiesToConsiderAfterExcludingUDs = getBookiesToConsiderAfterExcludingZones(
                    ensembleSize, writeQuorumSize, currentEnsemble, bookieToReplace, excludeBookies, zonesToExclude);

            if (bookiesToConsiderAfterExcludingUDs.isEmpty()) {
                zonesToExclude = getZonesToExcludeToMaintainMinZones(currentEnsemble, bookieToReplaceIndex,
                        writeQuorumSize);
                bookiesToConsiderAfterExcludingUDs = getBookiesToConsiderAfterExcludingZones(ensembleSize,
                        writeQuorumSize, currentEnsemble, bookieToReplace, excludeBookies, zonesToExclude);
            }
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    String getExcludedZonesString(Set<String> excludeZones) {
        StringBuilder excludedZonesString = new StringBuilder("~");
        boolean firstZone = true;
        for (String excludeZone : excludeZones) {
            if (!firstZone) {
                excludedZonesString.append(NetworkTopologyImpl.NODE_SEPARATOR);
            }
            excludedZonesString.append(excludeZone);
            firstZone = false;
        }
        return excludedZonesString.toString();
    }

    Set<BookieSocketAddress> getBookiesToConsider(String excludedZonesString, Set<BookieSocketAddress> excludeBookies) {
        Set<BookieSocketAddress> bookiesToConsider = new HashSet<BookieSocketAddress>();
        Set<Node> leaves = topology.getLeaves(excludedZonesString);
        for (Node leaf : leaves) {
            BookieSocketAddress bookieAddr = ((BookieNode) leaf).getAddr();
            if (excludeBookies.contains(bookieAddr)) {
                continue;
            }
            bookiesToConsider.add(bookieAddr);
        }
        return bookiesToConsider;
    }

    Set<BookieSocketAddress> getBookiesToConsiderAfterExcludingZones(int ensembleSize, int writeQuorumSize,
            List<BookieSocketAddress> currentEnsemble, BookieSocketAddress bookieToReplace,
            Set<BookieSocketAddress> excludeBookies, Set<String> excludeZones) {
        Set<BookieSocketAddress> bookiesToConsiderAfterExcludingZonesAndUDs = new HashSet<BookieSocketAddress>();
        HashMap<String, Set<String>> excludingUDsOfZonesToConsider = new HashMap<String, Set<String>>();
        int bookieToReplaceIndex = currentEnsemble.indexOf(bookieToReplace);
        Set<BookieSocketAddress> bookiesToConsiderAfterExcludingZones = getBookiesToConsider(
                getExcludedZonesString(excludeZones), excludeBookies);

        if (!bookiesToConsiderAfterExcludingZones.isEmpty()) {
            Set<String> zonesToConsider = getZonesOfBookies(bookiesToConsiderAfterExcludingZones);
            for (String zoneToConsider : zonesToConsider) {
                Set<String> upgradeDomainsOfAZoneInNeighboringNodes = getUpgradeDomainsOfAZoneInNeighboringNodes(
                        currentEnsemble, bookieToReplaceIndex, writeQuorumSize, zoneToConsider);
                excludingUDsOfZonesToConsider.put(zoneToConsider, upgradeDomainsOfAZoneInNeighboringNodes);
            }

            updateBookiesToConsiderAfterExcludingZonesAndUDs(bookiesToConsiderAfterExcludingZonesAndUDs,
                    bookiesToConsiderAfterExcludingZones, excludingUDsOfZonesToConsider);

            if (bookiesToConsiderAfterExcludingZonesAndUDs.isEmpty()) {
                excludingUDsOfZonesToConsider.clear();
                for (String zoneToConsider : zonesToConsider) {
                    Set<String> udsToExcludeToMaintainMinUDsInWriteQuorums = getUDsToExcludeToMaintainMinUDsInWriteQuorums(
                            currentEnsemble, bookieToReplaceIndex, writeQuorumSize, zoneToConsider);
                    excludingUDsOfZonesToConsider.put(zoneToConsider, udsToExcludeToMaintainMinUDsInWriteQuorums);
                }

                updateBookiesToConsiderAfterExcludingZonesAndUDs(bookiesToConsiderAfterExcludingZonesAndUDs,
                        bookiesToConsiderAfterExcludingZones, excludingUDsOfZonesToConsider);
            }
        }
        return bookiesToConsiderAfterExcludingZonesAndUDs;
    }

    void updateBookiesToConsiderAfterExcludingZonesAndUDs(Set<BookieSocketAddress> bookiesToConsiderAfterExcludingUDs,
            Set<BookieSocketAddress> bookiesToConsider, HashMap<String, Set<String>> excludingUDsOfZonesToConsider) {
        for (BookieSocketAddress bookieToConsider : bookiesToConsider) {
            NodePlacementInZone nodePlacement = getNodePlacementInZone(bookieToConsider);
            if (excludingUDsOfZonesToConsider.get(nodePlacement.getZone()).contains(nodePlacement.getUpgradeDomain())) {
                continue;
            }
            bookiesToConsiderAfterExcludingUDs.add(bookieToConsider);
        }
    }

    Set<String> getZonesOfNeighboringNodesInEnsemble(List<BookieSocketAddress> currentEnsemble, int indexOfNode,
            int numOfNeighboringNodes) {
        Set<String> zonesOfNeighboringNodes = new HashSet<String>();
        int ensembleSize = currentEnsemble.size();
        for (int i = (-1 * numOfNeighboringNodes); i <= numOfNeighboringNodes; i++) {
            if (i == 0) {
                continue;
            }
            int index = (indexOfNode + i + ensembleSize) % ensembleSize;
            BookieSocketAddress addrofNode = currentEnsemble.get(index);
            String zoneOfNode = getNodePlacementInZone(addrofNode).getZone();
            zonesOfNeighboringNodes.add(zoneOfNode);
        }
        return zonesOfNeighboringNodes;
    }

    Set<String> getZonesToExcludeToMaintainMinZones(List<BookieSocketAddress> currentEnsemble, int indexOfNode,
            int writeQuorumSize) {
        int ensSize = currentEnsemble.size();
        Set<String> zonesToExclude = new HashSet<String>();
        Set<String> zonesInWriteQuorum = new HashSet<String>();
        for (int i = -(writeQuorumSize - 1); i <= 0; i++) {
            zonesInWriteQuorum.clear();
            for (int j = 0; j < writeQuorumSize; j++) {
                int indexInEnsemble = (i + j + indexOfNode + ensSize) % ensSize;
                if (indexInEnsemble == indexOfNode) {
                    continue;
                }
                BookieSocketAddress bookieAddr = currentEnsemble.get(indexInEnsemble);
                NodePlacementInZone nodePlacement = getNodePlacementInZone(bookieAddr);
                zonesInWriteQuorum.add(nodePlacement.getZone());
            }
            if (zonesInWriteQuorum.size() == (minNumZonesPerWriteQuorum - 1)) {
                zonesToExclude.addAll(zonesInWriteQuorum);
            } else if (zonesInWriteQuorum.size() < (minNumZonesPerWriteQuorum - 1)) {
                LOG.error(
                        "For Ensemble: {} WriteQuorum of size: {} starting at: {} doesn't"
                                + " maintain minNumZonesPerWriteQuorum: {}",
                        currentEnsemble, writeQuorumSize, ((i + indexOfNode + ensSize) % ensSize),
                        minNumZonesPerWriteQuorum);
                throw new IllegalStateException("WriteQuorum doesn't maintain minNumZonesPerWriteQuorum");
            }
        }
        return zonesToExclude;
    }

    Set<String> getZonesOfBookies(Collection<BookieSocketAddress> bookieAddresses) {
        Set<String> zonesOfBookies = new HashSet<String>();
        for (BookieSocketAddress bookieAddress : bookieAddresses) {
            NodePlacementInZone nodePlacement = getNodePlacementInZone(bookieAddress);
            zonesOfBookies.add(nodePlacement.getZone());
        }
        return zonesOfBookies;
    }

    Set<String> getUpgradeDomainsOfAZoneInNeighboringNodes(List<BookieSocketAddress> currentEnsemble, int indexOfNode,
            int writeQuorumSize, String zone) {
        int ensSize = currentEnsemble.size();
        Set<String> upgradeDomainsOfAZoneInNeighboringNodes = new HashSet<String>();
        for (int i = -(writeQuorumSize - 1); i <= (writeQuorumSize - 1); i++) {
            if (i == 0) {
                continue;
            }
            int indexInEnsemble = (indexOfNode + i + ensSize) % ensSize;
            BookieSocketAddress bookieAddr = currentEnsemble.get(indexInEnsemble);
            NodePlacementInZone nodePlacement = getNodePlacementInZone(bookieAddr);
            if (nodePlacement.getZone().equals(zone)) {
                upgradeDomainsOfAZoneInNeighboringNodes.add(nodePlacement.getUpgradeDomain());
            }
        }
        return upgradeDomainsOfAZoneInNeighboringNodes;
    }

    Set<String> getUDsToExcludeToMaintainMinUDsInWriteQuorums(List<BookieSocketAddress> currentEnsemble,
            int indexOfNode, int writeQuorumSize, String zone) {
        int ensSize = currentEnsemble.size();
        Set<String> upgradeDomainsToExclude = new HashSet<String>();
        Set<String> upgradeDomainsOfThisZoneInWriteQuorum = new HashSet<String>();
        for (int i = -(writeQuorumSize - 1); i <= 0; i++) {
            upgradeDomainsOfThisZoneInWriteQuorum.clear();
            for (int j = 0; j < writeQuorumSize; j++) {
                int indexInEnsemble = (i + j + indexOfNode + ensSize) % ensSize;
                if (indexInEnsemble == indexOfNode) {
                    continue;
                }
                BookieSocketAddress bookieAddr = currentEnsemble.get(indexInEnsemble);
                NodePlacementInZone nodePlacement = getNodePlacementInZone(bookieAddr);
                if (nodePlacement.getZone().equals(zone)) {
                    upgradeDomainsOfThisZoneInWriteQuorum.add(nodePlacement.getUpgradeDomain());
                }
            }
            if (upgradeDomainsOfThisZoneInWriteQuorum.size() == 1) {
                upgradeDomainsToExclude.addAll(upgradeDomainsOfThisZoneInWriteQuorum);
            }
        }
        return upgradeDomainsToExclude;
    }

    @Override
    public void registerSlowBookie(BookieSocketAddress bookieSocketAddress, long entryId) {
        // TODO Auto-generated method stub
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(List<BookieSocketAddress> ensemble,
            BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        DistributionSchedule.WriteSet retList = reorderReadSequence(ensemble, bookiesHealthInfo, writeSet);
        retList.addMissingIndices(ensemble.size());
        return retList;
    }
}
