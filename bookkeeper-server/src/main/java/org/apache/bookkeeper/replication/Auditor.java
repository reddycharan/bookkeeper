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

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.AUDIT_BOOKIES_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.BOOKIE_TO_LEDGERS_MAP_CREATION_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.CHECK_ALL_LEDGERS_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BOOKIES_PER_LEDGER;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_FRAGMENTS_PER_LEDGER;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_CHECKED;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_UNDER_REPLICATED_LEDGERS;
import static org.apache.bookkeeper.replication.ReplicationStats.PLACEMENT_POLICY_CHECK_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICAS_CHECK_TIME;
import static org.apache.bookkeeper.replication.ReplicationStats.URL_PUBLISH_TIME_FOR_LOST_BOOKIE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auditor is a single entity in the entire Bookie cluster and will be watching
 * all the bookies under 'ledgerrootpath/available' zkpath. When any of the
 * bookie failed or disconnected from zk, he will start initiating the
 * re-replication activities by keeping all the corresponding ledgers of the
 * failed bookie as underreplicated znode in zk.
 *
 * <p>TODO: eliminate the direct usage of zookeeper here {@link https://github.com/apache/bookkeeper/issues/1332}
 */
@StatsDoc(
    name = AUDITOR_SCOPE,
    help = "Auditor related stats"
)
public class Auditor implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Auditor.class);
    private final ServerConfiguration conf;
    private final BookKeeper bkc;
    private final boolean ownBkc;
    private BookKeeperAdmin admin;
    private BookieLedgerIndexer bookieLedgerIndexer;
    private LedgerManager ledgerManager;
    private LedgerUnderreplicationManager ledgerUnderreplicationManager;
    private final ScheduledExecutorService executor;
    private List<String> knownBookies = new ArrayList<String>();
    private final String bookieIdentifier;
    private volatile Future<?> auditTask;
    private Set<String> bookiesToBeAudited = Sets.newHashSet();
    private volatile int lostBookieRecoveryDelayBeforeChange;
    private final AtomicInteger ledgersNotAdheringToPlacementPolicyGuageValue;
    private final AtomicInteger numOfLedgersFoundNotAdheringInPlacementPolicyCheck;
    private final AtomicInteger ledgersSoftlyAdheringToPlacementPolicyGuageValue;
    private final AtomicInteger numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck;
    private final AtomicInteger numOfClosedLedgersAuditedInPlacementPolicyCheck;
    private final AtomicInteger numOfURLedgersElapsedRecoveryGracePeriodGuageValue;
    private final AtomicInteger numOfURLedgersElapsedRecoveryGracePeriod;
    private final long underreplicatedLedgerRecoveryGracePeriod;
    private final int zkOpTimeoutMs;

    private final StatsLogger statsLogger;
    @StatsDoc(
        name = NUM_UNDER_REPLICATED_LEDGERS,
        help = "the distribution of num under_replicated ledgers on each auditor run"
    )
    private final OpStatsLogger numUnderReplicatedLedger;
    @StatsDoc(
        name = URL_PUBLISH_TIME_FOR_LOST_BOOKIE,
        help = "the latency distribution of publishing under replicated ledgers for lost bookies"
    )
    private final OpStatsLogger uRLPublishTimeForLostBookies;
    @StatsDoc(
        name = BOOKIE_TO_LEDGERS_MAP_CREATION_TIME,
        help = "the latency distribution of creating bookies-to-ledgers map"
    )
    private final OpStatsLogger bookieToLedgersMapCreationTime;
    @StatsDoc(
        name = CHECK_ALL_LEDGERS_TIME,
        help = "the latency distribution of checking all ledgers"
    )
    private final OpStatsLogger checkAllLedgersTime;
    @StatsDoc(
            name = PLACEMENT_POLICY_CHECK_TIME,
            help = "the latency distribution of placementPolicy check"
        )
    private final OpStatsLogger placementPolicyCheckTime;
    @StatsDoc(
            name = REPLICAS_CHECK_TIME,
            help = "the latency distribution of replicas check"
        )
    private final OpStatsLogger replicasCheckTime;
    @StatsDoc(
        name = AUDIT_BOOKIES_TIME,
        help = "the latency distribution of auditing all the bookies"
    )
    private final OpStatsLogger auditBookiesTime;
    @StatsDoc(
        name = NUM_LEDGERS_CHECKED,
        help = "the number of ledgers checked by the auditor"
    )
    private final Counter numLedgersChecked;
    @StatsDoc(
        name = NUM_FRAGMENTS_PER_LEDGER,
        help = "the distribution of number of fragments per ledger"
    )
    private final OpStatsLogger numFragmentsPerLedger;
    @StatsDoc(
        name = NUM_BOOKIES_PER_LEDGER,
        help = "the distribution of number of bookies per ledger"
    )
    private final OpStatsLogger numBookiesPerLedger;
    @StatsDoc(
        name = NUM_BOOKIE_AUDITS_DELAYED,
        help = "the number of bookie-audits delayed"
    )
    private final Counter numBookieAuditsDelayed;
    @StatsDoc(
        name = NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED,
        help = "the number of delayed-bookie-audits cancelled"
    )
    private final Counter numDelayedBookieAuditsCancelled;
    @StatsDoc(
            name = NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY,
            help = "Gauge for number of ledgers not adhering to placement policy found in placement policy check"
    )
    private final Gauge<Integer> numLedgersNotAdheringToPlacementPolicy;
    @StatsDoc(
            name = NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY,
            help = "Gauge for number of ledgers softly adhering to placement policy found in placement policy check"
    )
    private final Gauge<Integer> numLedgersSoftlyAdheringToPlacementPolicy;
    @StatsDoc(
            name = NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD,
            help = "Gauge for number of underreplicated ledgers elapsed recovery grace period"
    )
    private final Gauge<Integer> numUnderreplicatedLedgersElapsedRecoveryGracePeriod;

    static BookKeeper createBookKeeperClient(ServerConfiguration conf) throws InterruptedException, IOException {
        return createBookKeeperClient(conf, NullStatsLogger.INSTANCE);
    }

    static BookKeeper createBookKeeperClient(ServerConfiguration conf, StatsLogger statsLogger)
            throws InterruptedException, IOException {
        ClientConfiguration clientConfiguration = new ClientConfiguration(conf);
        clientConfiguration.setClientRole(ClientConfiguration.CLIENT_ROLE_SYSTEM);
        try {
            return BookKeeper.forConfig(clientConfiguration).statsLogger(statsLogger).build();
        } catch (BKException e) {
            throw new IOException("Failed to create bookkeeper client", e);
        }
    }

    static BookKeeper createBookKeeperClientThrowUnavailableException(ServerConfiguration conf)
        throws UnavailableException {
        try {
            return createBookKeeperClient(conf);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnavailableException("Failed to create bookkeeper client", e);
        } catch (IOException e) {
            throw new UnavailableException("Failed to create bookkeeper client", e);
        }
    }


    public Auditor(final String bookieIdentifier,
                   ServerConfiguration conf,
                   StatsLogger statsLogger)
        throws UnavailableException {
        this(
            bookieIdentifier,
            conf,
            createBookKeeperClientThrowUnavailableException(conf),
            true,
            statsLogger);
    }

    public Auditor(final String bookieIdentifier,
                   ServerConfiguration conf,
                   BookKeeper bkc,
                   boolean ownBkc,
                   StatsLogger statsLogger)
        throws UnavailableException {
        this.conf = conf;
        this.underreplicatedLedgerRecoveryGracePeriod = conf.getUnderreplicatedLedgerRecoveryGracePeriod();
        this.zkOpTimeoutMs = conf.getZkTimeout() * 2;
        this.bookieIdentifier = bookieIdentifier;
        this.statsLogger = statsLogger;
        this.numOfLedgersFoundNotAdheringInPlacementPolicyCheck = new AtomicInteger(0);
        this.ledgersNotAdheringToPlacementPolicyGuageValue = new AtomicInteger(0);
        this.numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck = new AtomicInteger(0);
        this.ledgersSoftlyAdheringToPlacementPolicyGuageValue = new AtomicInteger(0);
        this.numOfClosedLedgersAuditedInPlacementPolicyCheck = new AtomicInteger(0);
        this.numOfURLedgersElapsedRecoveryGracePeriod = new AtomicInteger(0);
        this.numOfURLedgersElapsedRecoveryGracePeriodGuageValue = new AtomicInteger(0);

        numUnderReplicatedLedger = this.statsLogger.getOpStatsLogger(ReplicationStats.NUM_UNDER_REPLICATED_LEDGERS);
        uRLPublishTimeForLostBookies = this.statsLogger
                .getOpStatsLogger(ReplicationStats.URL_PUBLISH_TIME_FOR_LOST_BOOKIE);
        bookieToLedgersMapCreationTime = this.statsLogger
                .getOpStatsLogger(ReplicationStats.BOOKIE_TO_LEDGERS_MAP_CREATION_TIME);
        checkAllLedgersTime = this.statsLogger.getOpStatsLogger(ReplicationStats.CHECK_ALL_LEDGERS_TIME);
        placementPolicyCheckTime = this.statsLogger.getOpStatsLogger(ReplicationStats.PLACEMENT_POLICY_CHECK_TIME);
        replicasCheckTime = this.statsLogger.getOpStatsLogger(ReplicationStats.REPLICAS_CHECK_TIME);
        auditBookiesTime = this.statsLogger.getOpStatsLogger(ReplicationStats.AUDIT_BOOKIES_TIME);
        numLedgersChecked = this.statsLogger.getCounter(ReplicationStats.NUM_LEDGERS_CHECKED);
        numFragmentsPerLedger = statsLogger.getOpStatsLogger(ReplicationStats.NUM_FRAGMENTS_PER_LEDGER);
        numBookiesPerLedger = statsLogger.getOpStatsLogger(ReplicationStats.NUM_BOOKIES_PER_LEDGER);
        numBookieAuditsDelayed = this.statsLogger.getCounter(ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED);
        numDelayedBookieAuditsCancelled = this.statsLogger
                .getCounter(ReplicationStats.NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED);
        numLedgersNotAdheringToPlacementPolicy = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return ledgersNotAdheringToPlacementPolicyGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY,
                numLedgersNotAdheringToPlacementPolicy);
        numLedgersSoftlyAdheringToPlacementPolicy = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return ledgersSoftlyAdheringToPlacementPolicyGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY,
                numLedgersSoftlyAdheringToPlacementPolicy);

        numUnderreplicatedLedgersElapsedRecoveryGracePeriod = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return numOfURLedgersElapsedRecoveryGracePeriodGuageValue.get();
            }
        };
        this.statsLogger.registerGauge(ReplicationStats.NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD,
                numUnderreplicatedLedgersElapsedRecoveryGracePeriod);

        this.bkc = bkc;
        this.ownBkc = ownBkc;
        initialize(conf, bkc);

        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "AuditorBookie-" + bookieIdentifier);
                    t.setDaemon(true);
                    return t;
                }
            });
    }

    private void initialize(ServerConfiguration conf, BookKeeper bkc)
            throws UnavailableException {
        try {
            LedgerManagerFactory ledgerManagerFactory = AbstractZkLedgerManagerFactory
                    .newLedgerManagerFactory(
                        conf,
                        bkc.getMetadataClientDriver().getLayoutManager());
            ledgerManager = ledgerManagerFactory.newLedgerManager();
            this.bookieLedgerIndexer = new BookieLedgerIndexer(ledgerManager);

            this.ledgerUnderreplicationManager = ledgerManagerFactory
                    .newLedgerUnderreplicationManager();
            this.admin = new BookKeeperAdmin(bkc, statsLogger);
            LOG.info("AuthProvider used by the Auditor is {}",
                admin.getConf().getClientAuthProviderFactoryClass());
            if (this.ledgerUnderreplicationManager
                    .initializeLostBookieRecoveryDelay(conf.getLostBookieRecoveryDelay())) {
                LOG.info("Initializing lostBookieRecoveryDelay zNode to the conif value: {}",
                        conf.getLostBookieRecoveryDelay());
            } else {
                LOG.info("Valid lostBookieRecoveryDelay zNode is available, so not creating "
                        + "lostBookieRecoveryDelay zNode as part of Auditor initialization ");
            }
            lostBookieRecoveryDelayBeforeChange = this.ledgerUnderreplicationManager.getLostBookieRecoveryDelay();
        } catch (CompatibilityException ce) {
            throw new UnavailableException(
                    "CompatibilityException while initializing Auditor", ce);
        } catch (IOException | KeeperException ioe) {
            throw new UnavailableException(
                    "Exception while initializing Auditor", ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new UnavailableException(
                    "Interrupted while initializing Auditor", ie);
        }
    }

    private void submitShutdownTask() {
        synchronized (this) {
            LOG.info("Executing submitShutdownTask");
            if (executor.isShutdown()) {
                LOG.info("executor is already shutdown");
                return;
            }
            executor.submit(new Runnable() {
                public void run() {
                    synchronized (Auditor.this) {
                        LOG.info("Shutting down Auditor's Executor");
                        executor.shutdown();
                    }
                }
            });
        }
    }

    @VisibleForTesting
    synchronized Future<?> submitAuditTask() {
        if (executor.isShutdown()) {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            f.setException(new BKAuditException("Auditor shutting down"));
            return f;
        }
        return executor.submit(new Runnable() {
                @SuppressWarnings("unchecked")
                public void run() {
                    try {
                        waitIfLedgerReplicationDisabled();
                        int lostBookieRecoveryDelay = Auditor.this.ledgerUnderreplicationManager
                                .getLostBookieRecoveryDelay();
                        List<String> availableBookies = getAvailableBookies();

                        // casting to String, as knownBookies and availableBookies
                        // contains only String values
                        // find new bookies(if any) and update the known bookie list
                        Collection<String> newBookies = CollectionUtils.subtract(
                                availableBookies, knownBookies);
                        knownBookies.addAll(newBookies);
                        if (!bookiesToBeAudited.isEmpty() && knownBookies.containsAll(bookiesToBeAudited)) {
                            // the bookie, which went down earlier and had an audit scheduled for,
                            // has come up. So let us stop tracking it and cancel the audit. Since
                            // we allow delaying of audit when there is only one failed bookie,
                            // bookiesToBeAudited should just have 1 element and hence containsAll
                            // check should be ok
                            if (auditTask != null && auditTask.cancel(false)) {
                                auditTask = null;
                                numDelayedBookieAuditsCancelled.inc();
                            }
                            bookiesToBeAudited.clear();
                        }

                        // find lost bookies(if any)
                        bookiesToBeAudited.addAll(CollectionUtils.subtract(knownBookies, availableBookies));
                        if (bookiesToBeAudited.size() == 0) {
                            return;
                        }

                        knownBookies.removeAll(bookiesToBeAudited);
                        if (lostBookieRecoveryDelay == 0) {
                            startAudit(false);
                            bookiesToBeAudited.clear();
                            return;
                        }
                        if (bookiesToBeAudited.size() > 1) {
                            // if more than one bookie is down, start the audit immediately;
                            LOG.info("Multiple bookie failure; not delaying bookie audit. "
                                    + "Bookies lost now: {}; All lost bookies: {}",
                                    CollectionUtils.subtract(knownBookies, availableBookies),
                                    bookiesToBeAudited);
                            if (auditTask != null && auditTask.cancel(false)) {
                                auditTask = null;
                                numDelayedBookieAuditsCancelled.inc();
                            }
                            startAudit(false);
                            bookiesToBeAudited.clear();
                            return;
                        }
                        if (auditTask == null) {
                            // if there is no scheduled audit, schedule one
                            auditTask = executor.schedule(new Runnable() {
                                public void run() {
                                    startAudit(false);
                                    auditTask = null;
                                    bookiesToBeAudited.clear();
                                }
                            }, lostBookieRecoveryDelay, TimeUnit.SECONDS);
                            numBookieAuditsDelayed.inc();
                            LOG.info("Delaying bookie audit by {} secs for {}", lostBookieRecoveryDelay,
                                    bookiesToBeAudited);
                        }
                    } catch (BKException bke) {
                        LOG.error("Exception getting bookie list", bke);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.error("Interrupted while watching available bookies ", ie);
                    } catch (UnavailableException ue) {
                        LOG.error("Exception while watching available bookies", ue);
                    }
                }
            });
    }

    synchronized Future<?> submitLostBookieRecoveryDelayChangedEvent() {
        if (executor.isShutdown()) {
            SettableFuture<Void> f = SettableFuture.<Void> create();
            f.setException(new BKAuditException("Auditor shutting down"));
            return f;
        }
        return executor.submit(new Runnable() {
            int lostBookieRecoveryDelay = -1;
            public void run() {
                try {
                    waitIfLedgerReplicationDisabled();
                    lostBookieRecoveryDelay = Auditor.this.ledgerUnderreplicationManager
                            .getLostBookieRecoveryDelay();
                    // if there is pending auditTask, cancel the task. So that it can be rescheduled
                    // after new lostBookieRecoveryDelay period
                    if (auditTask != null) {
                        LOG.info("lostBookieRecoveryDelay period has been changed so canceling the pending AuditTask");
                        auditTask.cancel(false);
                        numDelayedBookieAuditsCancelled.inc();
                    }

                    // if lostBookieRecoveryDelay is set to its previous value then consider it as
                    // signal to trigger the Audit immediately.
                    if ((lostBookieRecoveryDelay == 0)
                            || (lostBookieRecoveryDelay == lostBookieRecoveryDelayBeforeChange)) {
                        LOG.info(
                                "lostBookieRecoveryDelay has been set to 0 or reset to its previous value, "
                                + "so starting AuditTask. Current lostBookieRecoveryDelay: {}, "
                                + "previous lostBookieRecoveryDelay: {}",
                                lostBookieRecoveryDelay, lostBookieRecoveryDelayBeforeChange);
                        startAudit(false);
                        auditTask = null;
                        bookiesToBeAudited.clear();
                    } else if (auditTask != null) {
                        LOG.info("lostBookieRecoveryDelay has been set to {}, so rescheduling AuditTask accordingly",
                                lostBookieRecoveryDelay);
                        auditTask = executor.schedule(new Runnable() {
                            public void run() {
                                startAudit(false);
                                auditTask = null;
                                bookiesToBeAudited.clear();
                            }
                        }, lostBookieRecoveryDelay, TimeUnit.SECONDS);
                        numBookieAuditsDelayed.inc();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted while for LedgersReplication to be enabled ", ie);
                } catch (UnavailableException ue) {
                    LOG.error("Exception while reading from ZK", ue);
                } finally {
                    if (lostBookieRecoveryDelay != -1) {
                        lostBookieRecoveryDelayBeforeChange = lostBookieRecoveryDelay;
                    }
                }
            }
        });
    }

    public void start() {
        LOG.info("I'm starting as Auditor Bookie. ID: {}", bookieIdentifier);
        // on startup watching available bookie and based on the
        // available bookies determining the bookie failures.
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }

            try {
                watchBookieChanges();
                knownBookies = getAvailableBookies();
            } catch (BKException bke) {
                LOG.error("Couldn't get bookie list, so exiting", bke);
                submitShutdownTask();
            }

            try {
                this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(new LostBookieRecoveryDelayChangedCb());
            } catch (UnavailableException ue) {
                LOG.error("Exception while registering for LostBookieRecoveryDelay change notification, so exiting",
                        ue);
                submitShutdownTask();
            }

            scheduleBookieCheckTask();
            scheduleCheckAllLedgersTask();
            schedulePlacementPolicyCheckTask();
            scheduleReplicasCheckTask();
        }
    }

    private void scheduleBookieCheckTask() {
        long bookieCheckInterval = conf.getAuditorPeriodicBookieCheckInterval();
        if (bookieCheckInterval == 0) {
            LOG.info("Auditor periodic bookie checking disabled, running once check now anyhow");
            executor.submit(bookieCheck);
        } else {
            LOG.info("Auditor periodic bookie checking enabled" + " 'auditorPeriodicBookieCheckInterval' {} seconds",
                    bookieCheckInterval);
            executor.scheduleAtFixedRate(bookieCheck, 0, bookieCheckInterval, TimeUnit.SECONDS);
        }
    }

    private void scheduleCheckAllLedgersTask(){
        long interval = conf.getAuditorPeriodicCheckInterval();

        if (interval > 0) {
            LOG.info("Auditor periodic ledger checking enabled" + " 'auditorPeriodicCheckInterval' {} seconds",
                    interval);

            long checkAllLedgersLastExecutedCTime;
            long durationSinceLastExecutionInSecs;
            long initialDelay;
            try {
                checkAllLedgersLastExecutedCTime = ledgerUnderreplicationManager.getCheckAllLedgersCTime();
            } catch (UnavailableException ue) {
                LOG.error("Got UnavailableException while trying to get checkAllLedgersCTime", ue);
                checkAllLedgersLastExecutedCTime = -1;
            }
            if (checkAllLedgersLastExecutedCTime == -1) {
                durationSinceLastExecutionInSecs = -1;
                initialDelay = 0;
            } else {
                durationSinceLastExecutionInSecs = (System.currentTimeMillis() - checkAllLedgersLastExecutedCTime)
                        / 1000;
                if (durationSinceLastExecutionInSecs < 0) {
                    // this can happen if there is no strict time ordering
                    durationSinceLastExecutionInSecs = 0;
                }
                initialDelay = durationSinceLastExecutionInSecs > interval ? 0
                        : (interval - durationSinceLastExecutionInSecs);
            }
            LOG.info(
                    "checkAllLedgers scheduling info.  checkAllLedgersLastExecutedCTime: {} "
                            + "durationSinceLastExecutionInSecs: {} initialDelay: {} interval: {}",
                    checkAllLedgersLastExecutedCTime, durationSinceLastExecutionInSecs, initialDelay, interval);

            executor.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    try {
                        if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                            LOG.info("Ledger replication disabled, skipping checkAllLedgers");
                            return;
                        }

                        Stopwatch stopwatch = Stopwatch.createStarted();
                        LOG.info("Starting checkAllLedgers");
                        checkAllLedgers();
                        long checkAllLedgersDuration = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                        LOG.info("Completed checkAllLedgers in {} milliSeconds", checkAllLedgersDuration);
                        checkAllLedgersTime.registerSuccessfulEvent(checkAllLedgersDuration, TimeUnit.MILLISECONDS);
                    } catch (KeeperException ke) {
                        LOG.error("Exception while running periodic check", ke);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.error("Interrupted while running periodic check", ie);
                    } catch (BKException bke) {
                        LOG.error("Exception running periodic check", bke);
                    } catch (IOException ioe) {
                        LOG.error("I/O exception running periodic check", ioe);
                    } catch (ReplicationException.UnavailableException ue) {
                        LOG.error("Underreplication manager unavailable running periodic check", ue);
                    }
                }
                }, initialDelay, interval, TimeUnit.SECONDS);
        } else {
            LOG.info("Periodic checking disabled");
        }
    }

    private void schedulePlacementPolicyCheckTask(){
        long interval = conf.getAuditorPeriodicPlacementPolicyCheckInterval();

        if (interval > 0) {
            LOG.info("Auditor periodic placement policy check enabled"
                    + " 'auditorPeriodicPlacementPolicyCheckInterval' {} seconds", interval);

            long placementPolicyCheckLastExecutedCTime;
            long durationSinceLastExecutionInSecs;
            long initialDelay;
            try {
                placementPolicyCheckLastExecutedCTime = ledgerUnderreplicationManager.getPlacementPolicyCheckCTime();
            } catch (UnavailableException ue) {
                LOG.error("Got UnavailableException while trying to get placementPolicyCheckCTime", ue);
                placementPolicyCheckLastExecutedCTime = -1;
            }
            if (placementPolicyCheckLastExecutedCTime == -1) {
                durationSinceLastExecutionInSecs = -1;
                initialDelay = 0;
            } else {
                durationSinceLastExecutionInSecs = (System.currentTimeMillis() - placementPolicyCheckLastExecutedCTime)
                        / 1000;
                if (durationSinceLastExecutionInSecs < 0) {
                    // this can happen if there is no strict time ordering
                    durationSinceLastExecutionInSecs = 0;
                }
                initialDelay = durationSinceLastExecutionInSecs > interval ? 0
                        : (interval - durationSinceLastExecutionInSecs);
            }
            LOG.info(
                    "placementPolicyCheck scheduling info.  placementPolicyCheckLastExecutedCTime: {} "
                            + "durationSinceLastExecutionInSecs: {} initialDelay: {} interval: {}",
                    placementPolicyCheckLastExecutedCTime, durationSinceLastExecutionInSecs, initialDelay, interval);

            executor.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    try {
                        Stopwatch stopwatch = Stopwatch.createStarted();
                        LOG.info("Starting PlacementPolicyCheck");
                        placementPolicyCheck();
                        long placementPolicyCheckDuration = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                        int numOfLedgersFoundNotAdheringInPlacementPolicyCheckValue =
                                numOfLedgersFoundNotAdheringInPlacementPolicyCheck.get();
                        int numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue =
                                numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck.get();
                        int numOfClosedLedgersAuditedInPlacementPolicyCheckValue =
                                numOfClosedLedgersAuditedInPlacementPolicyCheck.get();
                        int numOfURLedgersElapsedRecoveryGracePeriodValue =
                                numOfURLedgersElapsedRecoveryGracePeriod.get();
                        LOG.info(
                                "Completed placementPolicyCheck in {} milliSeconds."
                                        + " numOfClosedLedgersAuditedInPlacementPolicyCheck {}"
                                        + " numOfLedgersNotAdheringToPlacementPolicy {}"
                                        + " numOfLedgersSoftlyAdheringToPlacementPolicy {}"
                                        + " numOfURLedgersElapsedRecoveryGracePeriod {}",
                                placementPolicyCheckDuration, numOfClosedLedgersAuditedInPlacementPolicyCheckValue,
                                numOfLedgersFoundNotAdheringInPlacementPolicyCheckValue,
                                numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue,
                                numOfURLedgersElapsedRecoveryGracePeriodValue);
                        ledgersNotAdheringToPlacementPolicyGuageValue
                                .set(numOfLedgersFoundNotAdheringInPlacementPolicyCheckValue);
                        ledgersSoftlyAdheringToPlacementPolicyGuageValue
                                .set(numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue);
                        numOfURLedgersElapsedRecoveryGracePeriodGuageValue
                                .set(numOfURLedgersElapsedRecoveryGracePeriodValue);
                        placementPolicyCheckTime.registerSuccessfulEvent(placementPolicyCheckDuration,
                                TimeUnit.MILLISECONDS);
                    } catch (BKAuditException e) {
                        int numOfLedgersFoundInPlacementPolicyCheckValue =
                                numOfLedgersFoundNotAdheringInPlacementPolicyCheck.get();
                        if (numOfLedgersFoundInPlacementPolicyCheckValue > 0) {
                            /*
                             * Though there is BKAuditException while doing
                             * placementPolicyCheck, it found few ledgers not
                             * adhering to placement policy. So reporting it.
                             */
                            ledgersNotAdheringToPlacementPolicyGuageValue
                                    .set(numOfLedgersFoundInPlacementPolicyCheckValue);
                        }

                        int numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue =
                                numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck.get();
                        if (numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue > 0) {
                            /*
                             * Though there is BKAuditException while doing
                             * placementPolicyCheck, it found few ledgers softly
                             * adhering to placement policy. So reporting it.
                             */
                            ledgersSoftlyAdheringToPlacementPolicyGuageValue
                                    .set(numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue);
                        }

                        int numOfURLedgersElapsedRecoveryGracePeriodValue =
                                numOfURLedgersElapsedRecoveryGracePeriod.get();
                        if (numOfURLedgersElapsedRecoveryGracePeriodValue > 0) {
                            /*
                             * Though there is BKAuditException while doing
                             * placementPolicyCheck, it found few urledgers have
                             * elapsed recovery graceperiod. So reporting it.
                             */
                            numOfURLedgersElapsedRecoveryGracePeriodGuageValue
                                    .set(numOfURLedgersElapsedRecoveryGracePeriodValue);
                        }

                        LOG.error(
                                "BKAuditException running periodic placementPolicy check."
                                        + "numOfLedgersNotAdheringToPlacementPolicy {}, "
                                        + "numOfLedgersSoftlyAdheringToPlacementPolicy {},"
                                        + "numOfURLedgersElapsedRecoveryGracePeriod {}",
                                numOfLedgersFoundInPlacementPolicyCheckValue,
                                numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheckValue,
                                numOfURLedgersElapsedRecoveryGracePeriodValue, e);
                    }
                }
            }, initialDelay, interval, TimeUnit.SECONDS);
        } else {
            LOG.info("Periodic placementPolicy check disabled");
        }
    }

    private void scheduleReplicasCheckTask() {
        long interval = conf.getAuditorPeriodicReplicasCheckInterval();

        if (interval > 0) {
            LOG.info("Auditor periodic replicas check enabled" + " 'auditorReplicasCheckInterval' {} seconds",
                    interval);
            long replicasCheckLastExecutedCTime;
            long durationSinceLastExecutionInSecs;
            long initialDelay;
            try {
                replicasCheckLastExecutedCTime = ledgerUnderreplicationManager.getReplicasCheckCTime();
            } catch (UnavailableException ue) {
                LOG.error("Got UnavailableException while trying to get replicasCheckCTime", ue);
                replicasCheckLastExecutedCTime = -1;
            }
            if (replicasCheckLastExecutedCTime == -1) {
                durationSinceLastExecutionInSecs = -1;
                initialDelay = 0;
            } else {
                durationSinceLastExecutionInSecs = (System.currentTimeMillis() - replicasCheckLastExecutedCTime) / 1000;
                if (durationSinceLastExecutionInSecs < 0) {
                    // this can happen if there is no strict time ordering
                    durationSinceLastExecutionInSecs = 0;
                }
                initialDelay = durationSinceLastExecutionInSecs > interval ? 0
                        : (interval - durationSinceLastExecutionInSecs);
            }
            LOG.info(
                    "replicasCheck scheduling info. replicasCheckLastExecutedCTime: {} "
                            + "durationSinceLastExecutionInSecs: {} initialDelay: {} interval: {}",
                    replicasCheckLastExecutedCTime, durationSinceLastExecutionInSecs, initialDelay, interval);

            executor.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    try {
                        Stopwatch stopwatch = Stopwatch.createStarted();
                        LOG.info("Starting ReplicasCheck");
                        replicasCheck();
                        long replicasCheckDuration = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                        LOG.info("Completed ReplicasCheck in {} milliSeconds.", replicasCheckDuration);
                        replicasCheckTime.registerSuccessfulEvent(replicasCheckDuration, TimeUnit.MILLISECONDS);
                    } catch (BKAuditException e) {
                        LOG.error("BKAuditException running periodic replicas check.", e);
                    }
                }
            }, initialDelay, interval, TimeUnit.SECONDS);
        } else {
            LOG.info("Periodic replicas check disabled");
        }
    }

    private class LostBookieRecoveryDelayChangedCb implements GenericCallback<Void> {
        @Override
        public void operationComplete(int rc, Void result) {
            try {
                Auditor.this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(LostBookieRecoveryDelayChangedCb.this);
            } catch (UnavailableException ae) {
                LOG.error("Exception while registering for a LostBookieRecoveryDelay notification", ae);
            }
            Auditor.this.submitLostBookieRecoveryDelayChangedEvent();
        }
    }

    private void waitIfLedgerReplicationDisabled() throws UnavailableException,
            InterruptedException {
        ReplicationEnableCb cb = new ReplicationEnableCb();
        if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
            LOG.info("LedgerReplication is disabled externally through Zookeeper, "
                    + "since DISABLE_NODE ZNode is created, so waiting untill it is enabled");
            ledgerUnderreplicationManager.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }

    private List<String> getAvailableBookies() throws BKException {
        // Get the available bookies
        Collection<BookieSocketAddress> availableBkAddresses = admin.getAvailableBookies();
        Collection<BookieSocketAddress> readOnlyBkAddresses = admin.getReadOnlyBookies();
        availableBkAddresses.addAll(readOnlyBkAddresses);

        List<String> availableBookies = new ArrayList<String>();
        for (BookieSocketAddress addr : availableBkAddresses) {
            availableBookies.add(addr.toString());
        }
        return availableBookies;
    }

    private void watchBookieChanges() throws BKException {
        admin.watchWritableBookiesChanged(bookies -> submitAuditTask());
        admin.watchReadOnlyBookiesChanged(bookies -> submitAuditTask());
    }

    /**
     * Start running the actual audit task.
     *
     * @param shutDownTask
     *      A boolean that indicates whether or not to schedule shutdown task on any failure
     */
    private void startAudit(boolean shutDownTask) {
        try {
            auditBookies();
            shutDownTask = false;
        } catch (BKException bke) {
            LOG.error("Exception getting bookie list", bke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while watching available bookies ", ie);
        } catch (BKAuditException bke) {
            LOG.error("Exception while watching available bookies", bke);
        }
        if (shutDownTask) {
            submitShutdownTask();
        }
    }

    @SuppressWarnings("unchecked")
    private void auditBookies()
            throws BKAuditException, InterruptedException, BKException {
        try {
            waitIfLedgerReplicationDisabled();
        } catch (UnavailableException ue) {
            LOG.error("Underreplication unavailable, skipping audit."
                      + "Will retry after a period");
            return;
        }
        LOG.info("Starting auditBookies");
        Stopwatch stopwatch = Stopwatch.createStarted();
        // put exit cases here
        Map<String, Set<Long>> ledgerDetails = generateBookie2LedgersIndex();
        try {
            if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                // has been disabled while we were generating the index
                // discard this run, and schedule a new one
                executor.submit(bookieCheck);
                return;
            }
        } catch (UnavailableException ue) {
            LOG.error("Underreplication unavailable, skipping audit."
                      + "Will retry after a period");
            return;
        }

        List<String> availableBookies = getAvailableBookies();
        // find lost bookies
        Set<String> knownBookies = ledgerDetails.keySet();
        Collection<String> lostBookies = CollectionUtils.subtract(knownBookies,
                availableBookies);

        bookieToLedgersMapCreationTime.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MILLISECONDS),
                TimeUnit.MILLISECONDS);
        if (lostBookies.size() > 0) {
            try {
                FutureUtils.result(
                    handleLostBookiesAsync(lostBookies, ledgerDetails), ReplicationException.EXCEPTION_HANDLER);
            } catch (ReplicationException e) {
                throw new BKAuditException(e.getMessage(), e.getCause());
            }
            uRLPublishTimeForLostBookies.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS);
        }
        LOG.info("Completed auditBookies");
        auditBookiesTime.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS),
                TimeUnit.MILLISECONDS);
    }

    private Map<String, Set<Long>> generateBookie2LedgersIndex()
            throws BKAuditException {
        return bookieLedgerIndexer.getBookieToLedgerIndex();
    }

    private CompletableFuture<?> handleLostBookiesAsync(Collection<String> lostBookies,
                                                        Map<String, Set<Long>> ledgerDetails) {
        LOG.info("Following are the failed bookies: {},"
                + " and searching its ledgers for re-replication", lostBookies);

        return FutureUtils.processList(
            Lists.newArrayList(lostBookies),
            bookieIP -> publishSuspectedLedgersAsync(
                Lists.newArrayList(bookieIP), ledgerDetails.get(bookieIP)),
            null
        );
    }

    private CompletableFuture<?> publishSuspectedLedgersAsync(Collection<String> missingBookies, Set<Long> ledgers) {
        if (null == ledgers || ledgers.size() == 0) {
            // there is no ledgers available for this bookie and just
            // ignoring the bookie failures
            LOG.info("There is no ledgers for the failed bookie: {}", missingBookies);
            return FutureUtils.Void();
        }
        LOG.info("Following ledgers: {} of bookie: {} are identified as underreplicated", ledgers, missingBookies);
        numUnderReplicatedLedger.registerSuccessfulValue(ledgers.size());
        return FutureUtils.processList(
            Lists.newArrayList(ledgers),
            ledgerId -> ledgerUnderreplicationManager.markLedgerUnderreplicatedAsync(ledgerId, missingBookies),
            null
        );
    }

    /**
     * Process the result returned from checking a ledger.
     */
    private class ProcessLostFragmentsCb implements GenericCallback<Set<LedgerFragment>> {
        final LedgerHandle lh;
        final AsyncCallback.VoidCallback callback;

        ProcessLostFragmentsCb(LedgerHandle lh, AsyncCallback.VoidCallback callback) {
            this.lh = lh;
            this.callback = callback;
        }

        public void operationComplete(int rc, Set<LedgerFragment> fragments) {
            if (rc == BKException.Code.OK) {
                Set<BookieSocketAddress> bookies = Sets.newHashSet();
                for (LedgerFragment f : fragments) {
                    bookies.addAll(f.getAddresses());
                }
                if (bookies.isEmpty()) {
                    // no missing fragments
                    callback.processResult(Code.OK, null, null);
                    return;
                }
                publishSuspectedLedgersAsync(
                    bookies.stream().map(BookieSocketAddress::toString).collect(Collectors.toList()),
                    Sets.newHashSet(lh.getId())
                ).whenComplete((result, cause) -> {
                    if (null != cause) {
                        LOG.error("Auditor exception publishing suspected ledger {} with lost bookies {}",
                            lh.getId(), bookies, cause);
                        callback.processResult(Code.ReplicationException, null, null);
                    } else {
                        callback.processResult(Code.OK, null, null);
                    }
                });
            } else {
                callback.processResult(rc, null, null);
            }
            lh.closeAsync().whenComplete((result, cause) -> {
                if (null != cause) {
                    LOG.warn("Error closing ledger {} : {}", lh.getId(), cause.getMessage());
                }
            });
        }
    }

    /**
     * List all the ledgers and check them individually. This should not
     * be run very often.
     */
    void checkAllLedgers() throws BKException, IOException, InterruptedException, KeeperException {
        final BookKeeper localClient = createBookKeeperClient(conf);
        final BookKeeperAdmin localAdmin = new BookKeeperAdmin(localClient, statsLogger);

        try {
            final LedgerChecker checker = new LedgerChecker(localClient);

            final CompletableFuture<Void> processFuture = new CompletableFuture<>();

            Processor<Long> checkLedgersProcessor = (ledgerId, callback) -> {
                try {
                    if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.info("Ledger rereplication has been disabled, aborting periodic check");
                        FutureUtils.complete(processFuture, null);
                        return;
                    }
                } catch (UnavailableException ue) {
                    LOG.error("Underreplication manager unavailable running periodic check", ue);
                    FutureUtils.complete(processFuture, null);
                    return;
                }

                localAdmin.asyncOpenLedgerNoRecovery(ledgerId, (rc, lh, ctx) -> {
                    if (Code.OK == rc) {
                        checker.checkLedger(lh,
                                // the ledger handle will be closed after checkLedger is done.
                                new ProcessLostFragmentsCb(lh, callback),
                                conf.getAuditorLedgerVerificationPercentage());
                        // we collect the following stats to get a measure of the
                        // distribution of a single ledger within the bk cluster
                        // the higher the number of fragments/bookies, the more distributed it is
                        numFragmentsPerLedger.registerSuccessfulValue(lh.getNumFragments());
                        numBookiesPerLedger.registerSuccessfulValue(lh.getNumBookies());
                        numLedgersChecked.inc();
                    } else if (Code.NoSuchLedgerExistsOnMetadataServerException == rc) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ledger {} was deleted before we could check it", ledgerId);
                        }
                        callback.processResult(Code.OK, null, null);
                    } else {
                        LOG.error("Couldn't open ledger {} to check : {}", ledgerId, BKException.getMessage(rc));
                        callback.processResult(rc, null, null);
                    }
                }, null);
            };

            ledgerManager.asyncProcessLedgers(checkLedgersProcessor,
                (rc, path, ctx) -> {
                    if (Code.OK == rc) {
                        FutureUtils.complete(processFuture, null);
                    } else {
                        FutureUtils.completeExceptionally(processFuture, BKException.create(rc));
                    }
                }, null, BKException.Code.OK, BKException.Code.ReadException);
            FutureUtils.result(processFuture, BKException.HANDLER);
            try {
                ledgerUnderreplicationManager.setCheckAllLedgersCTime(System.currentTimeMillis());
            } catch (UnavailableException ue) {
                LOG.error("Got exception while trying to set checkAllLedgersCTime", ue);
            }
        } finally {
            localAdmin.close();
            localClient.close();
        }
    }

    void placementPolicyCheck() throws BKAuditException {
        final CountDownLatch placementPolicyCheckLatch = new CountDownLatch(1);
        this.numOfLedgersFoundNotAdheringInPlacementPolicyCheck.set(0);
        this.numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck.set(0);
        this.numOfClosedLedgersAuditedInPlacementPolicyCheck.set(0);
        this.numOfURLedgersElapsedRecoveryGracePeriod.set(0);
        if (this.underreplicatedLedgerRecoveryGracePeriod > 0) {
            Iterator<UnderreplicatedLedger> underreplicatedLedgersInfo = ledgerUnderreplicationManager
                    .listLedgersToRereplicate(null);
            List<Long> urLedgersElapsedRecoveryGracePeriod = new ArrayList<Long>();
            while (underreplicatedLedgersInfo.hasNext()) {
                UnderreplicatedLedger underreplicatedLedger = underreplicatedLedgersInfo.next();
                long underreplicatedLedgerMarkTimeInMilSecs = underreplicatedLedger.getCtime();
                if (underreplicatedLedgerMarkTimeInMilSecs != UnderreplicatedLedger.UNASSIGNED_CTIME) {
                    long elapsedTimeInSecs =
                            (System.currentTimeMillis() - underreplicatedLedgerMarkTimeInMilSecs) / 1000;
                    if (elapsedTimeInSecs > this.underreplicatedLedgerRecoveryGracePeriod) {
                        urLedgersElapsedRecoveryGracePeriod.add(underreplicatedLedger.getLedgerId());
                        numOfURLedgersElapsedRecoveryGracePeriod.incrementAndGet();
                    }
                }
            }
            LOG.error("Following Underreplicated ledgers have elapsed recovery graceperiod: {}",
                    urLedgersElapsedRecoveryGracePeriod);
        }
        Processor<Long> ledgerProcessor = new Processor<Long>() {
            @Override
            public void process(Long ledgerId, AsyncCallback.VoidCallback iterCallback) {
                ledgerManager.readLedgerMetadata(ledgerId).whenComplete((metadataVer, exception) -> {
                    if (exception == null) {
                        LedgerMetadata metadata = metadataVer.getValue();
                        int writeQuorumSize = metadata.getWriteQuorumSize();
                        int ackQuorumSize = metadata.getAckQuorumSize();
                        if (metadata.isClosed()) {
                            boolean foundSegmentNotAdheringToPlacementPolicy = false;
                            boolean foundSegmentSoftlyAdheringToPlacementPolicy = false;
                            for (Map.Entry<Long, ? extends List<BookieSocketAddress>> ensemble : metadata
                                    .getAllEnsembles().entrySet()) {
                                long startEntryIdOfSegment = ensemble.getKey();
                                List<BookieSocketAddress> ensembleOfSegment = ensemble.getValue();
                                PlacementPolicyAdherence segmentAdheringToPlacementPolicy = admin
                                        .isEnsembleAdheringToPlacementPolicy(ensembleOfSegment, writeQuorumSize,
                                                ackQuorumSize);
                                if (segmentAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                                    foundSegmentNotAdheringToPlacementPolicy = true;
                                    LOG.warn(
                                            "For ledger: {}, Segment starting at entry: {}, with ensemble: {} having "
                                                    + "writeQuorumSize: {} and ackQuorumSize: {} is not adhering to "
                                                    + "EnsemblePlacementPolicy",
                                            ledgerId, startEntryIdOfSegment, ensembleOfSegment, writeQuorumSize,
                                            ackQuorumSize);
                                } else if (segmentAdheringToPlacementPolicy == PlacementPolicyAdherence.MEETS_SOFT) {
                                    foundSegmentSoftlyAdheringToPlacementPolicy = true;
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(
                                                "For ledger: {}, Segment starting at entry: {}, with ensemble: {}"
                                                        + " having writeQuorumSize: {} and ackQuorumSize: {} is"
                                                        + " softly adhering to EnsemblePlacementPolicy",
                                                ledgerId, startEntryIdOfSegment, ensembleOfSegment, writeQuorumSize,
                                                ackQuorumSize);
                                    }
                                }
                            }
                            if (foundSegmentNotAdheringToPlacementPolicy) {
                                numOfLedgersFoundNotAdheringInPlacementPolicyCheck.incrementAndGet();
                            } else if (foundSegmentSoftlyAdheringToPlacementPolicy) {
                                numOfLedgersFoundSoftlyAdheringInPlacementPolicyCheck.incrementAndGet();
                            }
                            numOfClosedLedgersAuditedInPlacementPolicyCheck.incrementAndGet();
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Ledger: {} is not yet closed, so skipping the placementPolicy"
                                        + "check analysis for now", ledgerId);
                            }
                        }
                        iterCallback.processResult(BKException.Code.OK, null, null);
                    } else if (BKException.getExceptionCode(exception)
                            == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                        LOG.debug("Ignoring replication of already deleted ledger {}", ledgerId);
                        iterCallback.processResult(BKException.Code.OK, null, null);
                    } else {
                        LOG.warn("Unable to read the ledger: {} information", ledgerId);
                        iterCallback.processResult(BKException.getExceptionCode(exception), null, null);
                    }
                });
            }
        };
        // Reading the result after processing all the ledgers
        final List<Integer> resultCode = new ArrayList<Integer>(1);
        ledgerManager.asyncProcessLedgers(ledgerProcessor, new AsyncCallback.VoidCallback() {

            @Override
            public void processResult(int rc, String s, Object obj) {
                resultCode.add(rc);
                placementPolicyCheckLatch.countDown();
            }
        }, null, BKException.Code.OK, BKException.Code.ReadException);
        try {
            placementPolicyCheckLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BKAuditException("Exception while doing placementPolicy check", e);
        }
        if (!resultCode.contains(BKException.Code.OK)) {
            throw new BKAuditException("Exception while doing placementPolicy check",
                    BKException.create(resultCode.get(0)));
        }
        try {
            ledgerUnderreplicationManager.setPlacementPolicyCheckCTime(System.currentTimeMillis());
        } catch (UnavailableException ue) {
            LOG.error("Got exception while trying to set PlacementPolicyCheckCTime", ue);
        }
    }

    private static class MissingEntriesInfo {
        private final long ledgerId;
        private final Entry<Long, ? extends List<BookieSocketAddress>> segmentInfo;
        private final BookieSocketAddress bookieMissingEntries;
        private final List<Long> unavailableEntriesList;

        private MissingEntriesInfo(long ledgerId, Entry<Long, ? extends List<BookieSocketAddress>> segmentInfo,
                BookieSocketAddress bookieMissingEntries, List<Long> unavailableEntriesList) {
            this.ledgerId = ledgerId;
            this.segmentInfo = segmentInfo;
            this.bookieMissingEntries = bookieMissingEntries;
            this.unavailableEntriesList = unavailableEntriesList;
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public Entry<Long, ? extends List<BookieSocketAddress>> getSegmentInfo() {
            return segmentInfo;
        }

        public BookieSocketAddress getBookieMissingEntries() {
            return bookieMissingEntries;
        }

        public List<Long> getUnavailableEntriesList() {
            return unavailableEntriesList;
        }
    }

    private class ReadLedgerMetadataCallbackForReplicasCheck
            implements BiConsumer<Versioned<LedgerMetadata>, Throwable> {
        final long ledgerInRange;
        final MultiCallback mcbForThisLedgerRange;
        final ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithMissingEntries;
        final ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithUnavailableBookies;

        ReadLedgerMetadataCallbackForReplicasCheck(long ledgerInRange, MultiCallback mcbForThisLedgerRange,
                ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithMissingEntries,
                ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithUnavailableBookies) {
            this.ledgerInRange = ledgerInRange;
            this.mcbForThisLedgerRange = mcbForThisLedgerRange;
            this.ledgersWithMissingEntries = ledgersWithMissingEntries;
            this.ledgersWithUnavailableBookies = ledgersWithUnavailableBookies;
        }

        @Override
        public void accept(Versioned<LedgerMetadata> metadataVer, Throwable exception) {
            if (exception != null) {
                if (BKException
                        .getExceptionCode(exception) == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                    LOG.debug("Ignoring replicas check of already deleted ledger {}", ledgerInRange);
                    mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
                    return;
                } else {
                    LOG.warn("Unable to read the ledger: {} information", ledgerInRange, exception);
                    mcbForThisLedgerRange.processResult(BKException.getExceptionCode(exception), null, null);
                    return;
                }
            }

            LedgerMetadata metadata = metadataVer.getValue();
            if (!metadata.isClosed()) {
                LOG.debug("Ledger: {} is not yet closed, so skipping the replicas check analysis for now",
                        ledgerInRange);
                mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
                return;
            }

            if (metadata.getLastEntryId() == -1) {
                LOG.debug("Ledger: {} is closed but it doesn't has any entries, so skipping the replicas check",
                        ledgerInRange);
                mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
                return;
            }

            int writeQuorumSize = metadata.getWriteQuorumSize();
            int ackQuorumSize = metadata.getAckQuorumSize();
            int ensembleSize = metadata.getEnsembleSize();
            RoundRobinDistributionSchedule distributionSchedule = new RoundRobinDistributionSchedule(writeQuorumSize,
                    ackQuorumSize, ensembleSize);
            List<Entry<Long, ? extends List<BookieSocketAddress>>> segments = new LinkedList<>(
                    metadata.getAllEnsembles().entrySet());
            /*
             * since there are multiple segments, MultiCallback should be
             * created for (ensembleSize * segments.size()) calls.
             */
            MultiCallback mcbForThisLedger = new MultiCallback(ensembleSize * segments.size(), mcbForThisLedgerRange,
                    null, BKException.Code.OK, BKException.Code.ReadException);
            for (int segmentNum = 0; segmentNum < segments.size(); segmentNum++) {
                final Entry<Long, ? extends List<BookieSocketAddress>> segmentEntry = segments.get(segmentNum);
                final List<BookieSocketAddress> ensembleOfSegment = segmentEntry.getValue();
                final long startEntryIdOfSegment = segmentEntry.getKey();
                final long lastEntryIdOfSegment = (segmentNum == (segments.size() - 1)) ? metadata.getLastEntryId()
                        : segments.get(segmentNum + 1).getKey() - 1;
                for (int bookieIndex = 0; bookieIndex < ensembleOfSegment.size(); bookieIndex++) {
                    final BookieSocketAddress bookieInEnsemble = ensembleOfSegment.get(bookieIndex);
                    final BitSet entriesStripedToThisBookie = distributionSchedule
                            .getEntriesStripedToTheBookie(bookieIndex, startEntryIdOfSegment, lastEntryIdOfSegment);
                    if (entriesStripedToThisBookie.cardinality() == 0) {
                        /*
                         * if no entry is expected to contain in this bookie,
                         * then there is no point in making
                         * getListOfEntriesOfLedger call for this bookie. So
                         * instead callback with success result.
                         */
                        mcbForThisLedger.processResult(BKException.Code.OK, null, null);
                        continue;
                    }
                    admin.asyncGetListOfEntriesOfLedger(bookieInEnsemble, ledgerInRange)
                            .whenComplete(new GetListOfEntriesOfLedgerCallbackForReplicasCheck(ledgerInRange,
                                    startEntryIdOfSegment, lastEntryIdOfSegment, bookieInEnsemble, segmentEntry,
                                    entriesStripedToThisBookie, ledgersWithMissingEntries,
                                    ledgersWithUnavailableBookies, mcbForThisLedger));
                }
            }
        }
    }

    private static class GetListOfEntriesOfLedgerCallbackForReplicasCheck
            implements BiConsumer<AvailabilityOfEntriesOfLedger, Throwable> {
        private long ledgerInRange;
        private long startEntryIdOfSegment;
        private long lastEntryIdOfSegment;
        private BookieSocketAddress bookieInEnsemble;
        private Entry<Long, ? extends List<BookieSocketAddress>> segmentEntry;
        private BitSet entriesStripedToThisBookie;
        private ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithMissingEntries;
        private ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithUnavailableBookies;
        private MultiCallback mcbForThisLedger;

        private GetListOfEntriesOfLedgerCallbackForReplicasCheck(long ledgerInRange, long startEntryIdOfSegment,
                long lastEntryIdOfSegment, BookieSocketAddress bookieInEnsemble,
                Entry<Long, ? extends List<BookieSocketAddress>> segmentEntry, BitSet entriesStripedToThisBookie,
                ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithMissingEntries,
                ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithUnavailableBookies,
                MultiCallback mcbForThisLedger) {
            this.ledgerInRange = ledgerInRange;
            this.startEntryIdOfSegment = startEntryIdOfSegment;
            this.lastEntryIdOfSegment = lastEntryIdOfSegment;
            this.bookieInEnsemble = bookieInEnsemble;
            this.segmentEntry = segmentEntry;
            this.entriesStripedToThisBookie = entriesStripedToThisBookie;
            this.ledgersWithMissingEntries = ledgersWithMissingEntries;
            this.ledgersWithUnavailableBookies = ledgersWithUnavailableBookies;
            this.mcbForThisLedger = mcbForThisLedger;
        }

        @Override
        public void accept(AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger,
                Throwable listOfEntriesException) {

            if (listOfEntriesException != null) {
                if (BKException
                        .getExceptionCode(listOfEntriesException) == BKException.Code.NoSuchLedgerExistsException) {
                    LOG.debug("Got NoSuchLedgerExistsException for ledger: {} from bookie: {}", ledgerInRange,
                            bookieInEnsemble);
                    /*
                     * in the case of NoSuchLedgerExistsException, it should be
                     * considered as empty AvailabilityOfEntriesOfLedger.
                     */
                    availabilityOfEntriesOfLedger = AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER;
                } else {
                    LOG.warn("Unable to GetListOfEntriesOfLedger for ledger: {} from: {}", ledgerInRange,
                            bookieInEnsemble, listOfEntriesException);
                    List<MissingEntriesInfo> unavailableBookiesInfoOfThisLedger = ledgersWithUnavailableBookies
                            .get(ledgerInRange);
                    if (unavailableBookiesInfoOfThisLedger == null) {
                        ledgersWithUnavailableBookies.putIfAbsent(ledgerInRange,
                                Collections.synchronizedList(new ArrayList<MissingEntriesInfo>()));
                        unavailableBookiesInfoOfThisLedger = ledgersWithUnavailableBookies.get(ledgerInRange);
                    }
                    unavailableBookiesInfoOfThisLedger
                    .add(new MissingEntriesInfo(ledgerInRange, segmentEntry, bookieInEnsemble, null));
                    /*
                     * here though GetListOfEntriesOfLedger has failed with
                     * exception, mcbForThisLedger should be called back with OK
                     * response, because we dont consider this as fatal error in
                     * replicasCheck and dont want replicasCheck to exit just
                     * because of this issue. So instead maintain the state of
                     * ledgersWithUnavailableBookies, so that replicascheck will
                     * report these ledgers/bookies appropriately.
                     */
                    mcbForThisLedger.processResult(BKException.Code.OK, null, null);
                    return;
                }
            }

            final List<Long> unavailableEntriesList = availabilityOfEntriesOfLedger
                    .getUnavailableEntries(startEntryIdOfSegment, lastEntryIdOfSegment, entriesStripedToThisBookie);
            if ((unavailableEntriesList != null) && (!unavailableEntriesList.isEmpty())) {
                List<MissingEntriesInfo> missingEntriesInfoOfThisLedger = ledgersWithMissingEntries.get(ledgerInRange);
                if (missingEntriesInfoOfThisLedger == null) {
                    ledgersWithMissingEntries.putIfAbsent(ledgerInRange,
                            Collections.synchronizedList(new ArrayList<MissingEntriesInfo>()));
                    missingEntriesInfoOfThisLedger = ledgersWithMissingEntries.get(ledgerInRange);
                }
                missingEntriesInfoOfThisLedger.add(
                        new MissingEntriesInfo(ledgerInRange, segmentEntry, bookieInEnsemble, unavailableEntriesList));
            }
            /*
             * here though unavailableEntriesList is not empty, mcbForThisLedger
             * should be called back with OK response, because we dont consider
             * this as fatal error in replicasCheck and dont want replicasCheck
             * to exit just because of this issue. So instead maintain the state
             * of missingEntriesInfoOfThisLedger, so that replicascheck will
             * report these ledgers/bookies/missingentries appropriately.
             */
            mcbForThisLedger.processResult(BKException.Code.OK, null, null);
        }
    }

    private static class ReplicasCheckFinalCallback implements AsyncCallback.VoidCallback {
        final AtomicInteger resultCode;
        final CountDownLatch replicasCheckLatch;

        private ReplicasCheckFinalCallback(AtomicInteger resultCode, CountDownLatch replicasCheckLatch) {
            this.resultCode = resultCode;
            this.replicasCheckLatch = replicasCheckLatch;
        }

        @Override
        public void processResult(int rc, String s, Object obj) {
            resultCode.set(rc);
            replicasCheckLatch.countDown();
        }
    }

    void replicasCheck() throws BKAuditException {
        ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithMissingEntries =
                    new ConcurrentHashMap<Long, List<MissingEntriesInfo>>();
        ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithUnavailableBookies =
                    new ConcurrentHashMap<Long, List<MissingEntriesInfo>>();
        LedgerRangeIterator ledgerRangeIterator = ledgerManager.getLedgerRanges(zkOpTimeoutMs);
        while (true) {
            LedgerRange ledgerRange = null;
            try {
                if (ledgerRangeIterator.hasNext()) {
                    ledgerRange = ledgerRangeIterator.next();
                } else {
                    break;
                }
            } catch (IOException ioe) {
                LOG.error("Got IOException while iterating LedgerRangeIterator", ioe);
                throw new BKAuditException("Got IOException while iterating LedgerRangeIterator", ioe);
            }
            ledgersWithMissingEntries.clear();
            ledgersWithUnavailableBookies.clear();
            Set<Long> ledgersInRange = ledgerRange.getLedgers();
            int numOfLedgersInRange = ledgersInRange.size();
            // Reading the result after processing all the ledgers
            final AtomicInteger resultCode = new AtomicInteger();
            final CountDownLatch replicasCheckLatch = new CountDownLatch(1);

            ReplicasCheckFinalCallback finalCB = new ReplicasCheckFinalCallback(resultCode, replicasCheckLatch);
            MultiCallback mcbForThisLedgerRange = new MultiCallback(numOfLedgersInRange, finalCB, null,
                    BKException.Code.OK, BKException.Code.ReadException);
            LOG.debug("Number of ledgers in the current LedgerRange : {}", numOfLedgersInRange);
            for (Long ledgerInRange : ledgersInRange) {
                if (checkUnderReplicationForReplicasCheck(ledgerInRange, mcbForThisLedgerRange)) {
                    /*
                     * if ledger is marked underreplicated, then ignore this
                     * ledger for replicascheck.
                     */
                    continue;
                }
                ledgerManager.readLedgerMetadata(ledgerInRange)
                        .whenComplete(new ReadLedgerMetadataCallbackForReplicasCheck(ledgerInRange,
                                mcbForThisLedgerRange, ledgersWithMissingEntries, ledgersWithUnavailableBookies));
            }
            try {
                /*
                 * if mcbForThisLedgerRange is not calledback within 90 secs
                 * then better give up doing replicascheck, since there could be
                 * an issue and blocking the single threaded auditor executor
                 * thread is not expected.
                 */
                if (!replicasCheckLatch.await(90, TimeUnit.SECONDS)) {
                    LOG.error("For LedgerRange with num of ledgers : {} it didn't complete replicascheck"
                            + " in 60 secs, so giving up", numOfLedgersInRange);
                    throw new BKAuditException("Got InterruptedException while doing replicascheck");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Got InterruptedException while doing replicascheck", ie);
                throw new BKAuditException("Got InterruptedException while doing replicascheck", ie);
            }
            reportLedgersWithMissingEntries(ledgersWithMissingEntries);
            reportLedgersWithUnavailableBookies(ledgersWithUnavailableBookies);
            int resultCodeIntValue = resultCode.get();
            if (resultCodeIntValue != BKException.Code.OK) {
                throw new BKAuditException("Exception while doing replicas check",
                        BKException.create(resultCodeIntValue));
            }
        }
        try {
            ledgerUnderreplicationManager.setReplicasCheckCTime(System.currentTimeMillis());
        } catch (UnavailableException ue) {
            LOG.error("Got exception while trying to set ReplicasCheckCTime", ue);
        }
    }

    private void reportLedgersWithMissingEntries(
            ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithMissingEntries) {
        StringBuilder errMessage = new StringBuilder();
        for (Map.Entry<Long, List<MissingEntriesInfo>> ledgerWithMissingEntriesInfo : ledgersWithMissingEntries
                .entrySet()) {
            errMessage.setLength(0);
            long ledgerWithMissingEntries = ledgerWithMissingEntriesInfo.getKey();
            List<MissingEntriesInfo> missingEntriesInfoList = ledgerWithMissingEntriesInfo.getValue();
            errMessage.append("Ledger : " + ledgerWithMissingEntries + " has following missing entries : ");
            for (int listInd = 0; listInd < missingEntriesInfoList.size(); listInd++) {
                MissingEntriesInfo missingEntriesInfo = missingEntriesInfoList.get(listInd);
                Entry<Long, ? extends List<BookieSocketAddress>> segmentInfo = missingEntriesInfo.getSegmentInfo();
                errMessage.append(
                        "In segment starting at " + segmentInfo.getKey() + " with ensemble " + segmentInfo.getValue()
                                + ", following entries " + missingEntriesInfo.getUnavailableEntriesList()
                                + " are missing in bookie: " + missingEntriesInfo.getBookieMissingEntries());
                if (listInd < (missingEntriesInfoList.size() - 1)) {
                    errMessage.append(", ");
                }
            }
            LOG.error(errMessage.toString());
        }
    }

    private void reportLedgersWithUnavailableBookies(
            ConcurrentHashMap<Long, List<MissingEntriesInfo>> ledgersWithUnavailableBookies) {
        StringBuilder errMessage = new StringBuilder();
        for (Map.Entry<Long, List<MissingEntriesInfo>> ledgerWithUnavailableBookiesInfo : ledgersWithUnavailableBookies
                .entrySet()) {
            errMessage.setLength(0);
            long ledgerWithUnavailableBookies = ledgerWithUnavailableBookiesInfo.getKey();
            List<MissingEntriesInfo> missingBookiesInfoList = ledgerWithUnavailableBookiesInfo.getValue();
            errMessage.append("Ledger : " + ledgerWithUnavailableBookies + " has following unavailable bookies : ");
            for (int listInd = 0; listInd < missingBookiesInfoList.size(); listInd++) {
                MissingEntriesInfo missingBookieInfo = missingBookiesInfoList.get(listInd);
                Entry<Long, ? extends List<BookieSocketAddress>> segmentInfo = missingBookieInfo.getSegmentInfo();
                errMessage.append("In segment starting at " + segmentInfo.getKey() + " with ensemble "
                        + segmentInfo.getValue() + ", following bookie has not responded "
                        + missingBookieInfo.getBookieMissingEntries());
                if (listInd < (missingBookiesInfoList.size() - 1)) {
                    errMessage.append(", ");
                }
            }
            LOG.error(errMessage.toString());
        }
    }

    boolean checkUnderReplicationForReplicasCheck(long ledgerInRange, VoidCallback mcbForThisLedgerRange) {
        try {
            if (ledgerUnderreplicationManager.getLedgerUnreplicationInfo(ledgerInRange) == null) {
                return false;
            }
            /*
             * this ledger is marked underreplicated, so ignore it for
             * replicasCheck.
             */
            LOG.debug("Ledger: {} is marked underrreplicated, ignore this ledger for replicasCheck",
                    ledgerInRange);
            mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
            return true;
        } catch (UnavailableException une) {
            LOG.error("Got exception while trying to check if ledger: {} is underreplicated", ledgerInRange, une);
            mcbForThisLedgerRange.processResult(BKException.getExceptionCode(une), null, null);
            return true;
        }
    }

    /**
     * Shutdown the auditor.
     */
    public void shutdown() {
        LOG.info("Shutting down auditor");
        executor.shutdown();
        try {
            while (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Executor not shutting down, interrupting");
                executor.shutdownNow();
            }
            admin.close();
            if (ownBkc) {
                bkc.close();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while shutting down auditor bookie", ie);
        } catch (BKException bke) {
            LOG.warn("Exception while shutting down auditor bookie", bke);
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Return true if auditor is running otherwise return false.
     *
     * @return auditor status
     */
    public boolean isRunning() {
        return !executor.isShutdown();
    }

    private final Runnable bookieCheck = new Runnable() {
            public void run() {
                if (auditTask == null) {
                    startAudit(true);
                } else {
                    // if due to a lost bookie an audit task was scheduled,
                    // let us not run this periodic bookie check now, if we
                    // went ahead, we'll report under replication and the user
                    // wanted to avoid that(with lostBookieRecoveryDelay option)
                    LOG.info("Audit already scheduled; skipping periodic bookie check");
                }
            }
        };

    int getLostBookieRecoveryDelayBeforeChange() {
        return lostBookieRecoveryDelayBeforeChange;
    }

    Future<?> getAuditTask() {
        return auditTask;
    }
}
