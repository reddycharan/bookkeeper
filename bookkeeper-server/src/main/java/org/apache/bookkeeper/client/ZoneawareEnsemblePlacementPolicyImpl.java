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
import java.util.Random;
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
    private final Random rand;
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

    @StatsDoc(name = FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER, help = "Counter for number of times"
            + " DNSResolverDecorator failed to resolve Network Location")
    protected Counter failedToResolveNetworkLocationCounter = null;
    @StatsDoc(name = NUM_WRITABLE_BOOKIES_IN_DEFAULT_FAULTDOMAIN, help = "Gauge for the number of writable"
            + " Bookies in default fault domain")
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
            repString = zone + upgradeDomain;
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
            NetworkTopology.DEFAULT_ZONE, NetworkTopology.DEFAULT_UPGRADEDOMAIN);

    ZoneawareEnsemblePlacementPolicyImpl() {
        super();
        address2NodePlacement = new ConcurrentHashMap<BookieSocketAddress, NodePlacementInZone>();
        rand = new Random(System.currentTimeMillis());
    }

    protected NodePlacementInZone getNodePlacementInZone(BookieSocketAddress addr) {
        NodePlacementInZone nodePlacement = address2NodePlacement.get(addr);
        if (null == nodePlacement) {
            String networkLocation = resolveNetworkLocation(addr);
            if (NetworkTopology.DEFAULT_ZONE_AND_UPGRADEDOMAIN.equals(networkLocation)) {
                nodePlacement = UNKNOWN_NODEPLACEMENT;
            } else {
                String[] parts = StringUtils.split(NodeBase.normalize(networkLocation), NodeBase.PATH_SEPARATOR);
                if (parts.length != 2) {
                    nodePlacement = UNKNOWN_NODEPLACEMENT;
                } else {
                    nodePlacement = new NodePlacementInZone(NodeBase.PATH_SEPARATOR_STR + parts[0],
                            NodeBase.PATH_SEPARATOR_STR + parts[1]);
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
            this.weightedSelection = new DynamicWeightedRandomSelectionImpl<BookieNode>(this.maxWeightMultiple);
            LOG.info("Weight based placement with max multiple of {}", this.maxWeightMultiple);
        } else {
            LOG.info("Not weighted");
        }
        this.minNumZonesPerWriteQuorum = conf.getMinNumZonesPerWriteQuorum();
        this.desiredNumZonesPerWriteQuorum = conf.getDesiredNumZonesPerWriteQuorum();
        if (minNumZonesPerWriteQuorum > desiredNumZonesPerWriteQuorum) {
            LOG.error(
                    "It is misconfigured, for ZoneawareEnsemblePlacementPolicy, minNumZonesPerWriteQuorum: {} cann't be"
                            + " greater than desiredNumZonesPerWriteQuorum: {}",
                    minNumZonesPerWriteQuorum, desiredNumZonesPerWriteQuorum);
            throw new IllegalArgumentException("minNumZonesPerWriteQuorum: " + minNumZonesPerWriteQuorum
                    + " cann't be greater than desiredNumZonesPerWriteQuorum: " + desiredNumZonesPerWriteQuorum);
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
        int desiredNumZonesPerWriteQuorumForThisEnsemble = (writeQuorumSize < desiredNumZonesPerWriteQuorum)
                ? writeQuorumSize : desiredNumZonesPerWriteQuorum;
        List<BookieSocketAddress> newEnsemble = new ArrayList<BookieSocketAddress>(
                Collections.nCopies(ensembleSize, null));
        rwLock.readLock().lock();
        try {
            Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = addDefaultFaultDomainBookies(excludeBookies);
            for (int index = 0; index < ensembleSize; index++) {
                setBookieInTheEnsemble(ensembleSize, writeQuorumSize, newEnsemble, newEnsemble, index,
                        desiredNumZonesPerWriteQuorumForThisEnsemble, comprehensiveExclusionBookiesSet);
            }
            return PlacementResult.of(newEnsemble,
                    isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public PlacementResult<BookieSocketAddress> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        int bookieToReplaceIndex = currentEnsemble.indexOf(bookieToReplace);
        int desiredNumZonesPerWriteQuorumForThisEnsemble = (writeQuorumSize < desiredNumZonesPerWriteQuorum)
                ? writeQuorumSize : desiredNumZonesPerWriteQuorum;
        List<BookieSocketAddress> newEnsemble = new ArrayList<BookieSocketAddress>(currentEnsemble);
        rwLock.readLock().lock();
        try {
            Set<BookieSocketAddress> comprehensiveExclusionBookiesSet = addDefaultFaultDomainBookies(excludeBookies);
            BookieSocketAddress candidateAddr = setBookieInTheEnsemble(ensembleSize, writeQuorumSize, currentEnsemble,
                    newEnsemble, bookieToReplaceIndex, desiredNumZonesPerWriteQuorumForThisEnsemble, comprehensiveExclusionBookiesSet);
            return PlacementResult.of(candidateAddr,
                    isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private BookieSocketAddress setBookieInTheEnsemble(int ensembleSize, int writeQuorumSize,
            List<BookieSocketAddress> currentEnsemble, List<BookieSocketAddress> newEnsemble, int bookieToReplaceIndex,
            int desiredNumZonesPerWriteQuorumForThisEnsemble, Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        BookieSocketAddress bookieToReplace = currentEnsemble.get(bookieToReplaceIndex);
        Set<String> zonesToExclude = getZonesOfNeighboringNodesInEnsemble(currentEnsemble, bookieToReplaceIndex,
                (desiredNumZonesPerWriteQuorumForThisEnsemble - 1));
        Set<BookieNode> bookiesToConsiderAfterExcludingUDs = getBookiesToConsiderAfterExcludingZones(ensembleSize,
                writeQuorumSize, currentEnsemble, bookieToReplaceIndex, excludeBookies, zonesToExclude);

        if (bookiesToConsiderAfterExcludingUDs.isEmpty()) {
            zonesToExclude = getZonesToExcludeToMaintainMinZones(currentEnsemble, bookieToReplaceIndex,
                    writeQuorumSize);
            bookiesToConsiderAfterExcludingUDs = getBookiesToConsiderAfterExcludingZones(ensembleSize, writeQuorumSize,
                    currentEnsemble, bookieToReplaceIndex, excludeBookies, zonesToExclude);
        }
        if (bookiesToConsiderAfterExcludingUDs.isEmpty()) {
            LOG.error("Not enough bookies are available to replaceBookie : {} in ensemble : {} with excludeBookies {}.",
                    bookieToReplace, currentEnsemble, excludeBookies);
            throw new BKNotEnoughBookiesException();
        }

        BookieSocketAddress candidateAddr = selectCandidateNode(bookiesToConsiderAfterExcludingUDs).getAddr();
        if (currentEnsemble.isEmpty()) {
            /*
             * in testing code there are test cases which would pass empty
             * currentEnsemble
             */
            newEnsemble.add(candidateAddr);
        } else {
            newEnsemble.set(bookieToReplaceIndex, candidateAddr);
        }
        return candidateAddr;
    }

    /*
     * this method should be called in readlock scope of 'rwLock'
     */
    protected Set<BookieSocketAddress> addDefaultFaultDomainBookies(Set<BookieSocketAddress> excludeBookies) {
        Set<BookieSocketAddress> comprehensiveExclusionBookiesSet;
        Set<BookieSocketAddress> bookiesInDefaultFaultDomain = null;
        Set<Node> defaultFaultDomainLeaves = topology.getLeaves(getDefaultFaultDomain());
        for (Node node : defaultFaultDomainLeaves) {
            if (node instanceof BookieNode) {
                if (bookiesInDefaultFaultDomain == null) {
                    bookiesInDefaultFaultDomain = new HashSet<BookieSocketAddress>(excludeBookies);
                }
                bookiesInDefaultFaultDomain.add(((BookieNode) node).getAddr());
            } else {
                LOG.error("found non-BookieNode: {} as leaf of defaultFaultDomain: {}", node, getDefaultFaultDomain());
            }
        }
        if ((bookiesInDefaultFaultDomain == null) || bookiesInDefaultFaultDomain.isEmpty()) {
            comprehensiveExclusionBookiesSet = excludeBookies;
        } else {
            comprehensiveExclusionBookiesSet = new HashSet<BookieSocketAddress>(excludeBookies);
            comprehensiveExclusionBookiesSet.addAll(bookiesInDefaultFaultDomain);
        }
        return comprehensiveExclusionBookiesSet;
    }

    private BookieNode selectCandidateNode(Set<BookieNode> bookiesToConsiderAfterExcludingUDs) {
        BookieNode candidate = null;
        if (!this.isWeighted) {
            int randSelIndex = rand.nextInt(bookiesToConsiderAfterExcludingUDs.size());
            int ind = 0;
            for (BookieNode bookieNode : bookiesToConsiderAfterExcludingUDs) {
                if (ind == randSelIndex) {
                    candidate = bookieNode;
                    break;
                }
                ind++;
            }
        } else {
            candidate = weightedSelection.getNextRandom(bookiesToConsiderAfterExcludingUDs);
        }
        return candidate;
    }

    private String getExcludedZonesString(Set<String> excludeZones) {
        if (excludeZones.isEmpty()) {
            return "";
        }
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

    private Set<BookieNode> getBookiesToConsider(String excludedZonesString, Set<BookieSocketAddress> excludeBookies) {
        Set<BookieNode> bookiesToConsider = new HashSet<BookieNode>();
        Set<Node> leaves = topology.getLeaves(excludedZonesString);
        for (Node leaf : leaves) {
            BookieNode bookieNode = ((BookieNode) leaf);
            if (excludeBookies.contains(bookieNode.getAddr())) {
                continue;
            }
            bookiesToConsider.add(bookieNode);
        }
        return bookiesToConsider;
    }

    private Set<BookieNode> getBookiesToConsiderAfterExcludingZones(int ensembleSize, int writeQuorumSize,
            List<BookieSocketAddress> currentEnsemble, int bookieToReplaceIndex,
            Set<BookieSocketAddress> excludeBookies, Set<String> excludeZones) {
        Set<BookieNode> bookiesToConsiderAfterExcludingZonesAndUDs = new HashSet<BookieNode>();
        HashMap<String, Set<String>> excludingUDsOfZonesToConsider = new HashMap<String, Set<String>>();
        Set<BookieNode> bookiesToConsiderAfterExcludingZones = getBookiesToConsider(
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
                    Set<String> udsToExcludeToMaintainMinUDsInWriteQuorums =
                            getUDsToExcludeToMaintainMinUDsInWriteQuorums(currentEnsemble, bookieToReplaceIndex,
                                    writeQuorumSize, zoneToConsider);
                    excludingUDsOfZonesToConsider.put(zoneToConsider, udsToExcludeToMaintainMinUDsInWriteQuorums);
                }

                updateBookiesToConsiderAfterExcludingZonesAndUDs(bookiesToConsiderAfterExcludingZonesAndUDs,
                        bookiesToConsiderAfterExcludingZones, excludingUDsOfZonesToConsider);
            }
        }
        return bookiesToConsiderAfterExcludingZonesAndUDs;
    }

    private void updateBookiesToConsiderAfterExcludingZonesAndUDs(Set<BookieNode> bookiesToConsiderAfterExcludingUDs,
            Set<BookieNode> bookiesToConsider, HashMap<String, Set<String>> excludingUDsOfZonesToConsider) {
        for (BookieNode bookieToConsider : bookiesToConsider) {
            NodePlacementInZone nodePlacement = getLocalNodePlacementInZone(bookieToConsider);
            if (excludingUDsOfZonesToConsider.get(nodePlacement.getZone()).contains(nodePlacement.getUpgradeDomain())) {
                continue;
            }
            bookiesToConsiderAfterExcludingUDs.add(bookieToConsider);
        }
    }

    private Set<String> getZonesOfNeighboringNodesInEnsemble(List<BookieSocketAddress> currentEnsemble, int indexOfNode,
            int numOfNeighboringNodes) {
        Set<String> zonesOfNeighboringNodes = new HashSet<String>();
        int ensembleSize = currentEnsemble.size();
        for (int i = (-1 * numOfNeighboringNodes); i <= numOfNeighboringNodes; i++) {
            if (i == 0) {
                continue;
            }
            int index = (indexOfNode + i + ensembleSize) % ensembleSize;
            BookieSocketAddress addrofNode = currentEnsemble.get(index);
            if (addrofNode == null) {
                continue;
            }
            String zoneOfNode = getNodePlacementInZone(addrofNode).getZone();
            zonesOfNeighboringNodes.add(zoneOfNode);
        }
        return zonesOfNeighboringNodes;
    }

    private Set<String> getZonesToExcludeToMaintainMinZones(List<BookieSocketAddress> currentEnsemble, int indexOfNode,
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
                if (bookieAddr == null) {
                    continue;
                }
                NodePlacementInZone nodePlacement = getNodePlacementInZone(bookieAddr);
                zonesInWriteQuorum.add(nodePlacement.getZone());
            }
            if (zonesInWriteQuorum.size() <= (minNumZonesPerWriteQuorum - 1)) {
                zonesToExclude.addAll(zonesInWriteQuorum);
            }
        }
        return zonesToExclude;
    }

    private Set<String> getZonesOfBookies(Collection<BookieNode> bookieNodes) {
        Set<String> zonesOfBookies = new HashSet<String>();
        for (BookieNode bookieNode : bookieNodes) {
            NodePlacementInZone nodePlacement = getLocalNodePlacementInZone(bookieNode);
            zonesOfBookies.add(nodePlacement.getZone());
        }
        return zonesOfBookies;
    }

    private Set<String> getUpgradeDomainsOfAZoneInNeighboringNodes(List<BookieSocketAddress> currentEnsemble,
            int indexOfNode, int writeQuorumSize, String zone) {
        int ensSize = currentEnsemble.size();
        Set<String> upgradeDomainsOfAZoneInNeighboringNodes = new HashSet<String>();
        for (int i = -(writeQuorumSize - 1); i <= (writeQuorumSize - 1); i++) {
            if (i == 0) {
                continue;
            }
            int indexInEnsemble = (indexOfNode + i + ensSize) % ensSize;
            BookieSocketAddress bookieAddr = currentEnsemble.get(indexInEnsemble);
            if (bookieAddr == null) {
                continue;
            }
            NodePlacementInZone nodePlacement = getNodePlacementInZone(bookieAddr);
            if (nodePlacement.getZone().equals(zone)) {
                upgradeDomainsOfAZoneInNeighboringNodes.add(nodePlacement.getUpgradeDomain());
            }
        }
        return upgradeDomainsOfAZoneInNeighboringNodes;
    }

    private Set<String> getUDsToExcludeToMaintainMinUDsInWriteQuorums(List<BookieSocketAddress> currentEnsemble,
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
                if (bookieAddr == null) {
                    continue;
                }
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

    @Override
    public boolean isEnsembleAdheringToPlacementPolicy(List<BookieSocketAddress> ensembleList, int writeQuorumSize,
            int ackQuorumSize) {
        return true;
    }
}
