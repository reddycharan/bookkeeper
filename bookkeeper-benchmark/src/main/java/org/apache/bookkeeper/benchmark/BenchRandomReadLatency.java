/*
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
package org.apache.bookkeeper.benchmark;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;

import java.util.Enumeration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import static com.google.common.base.Charsets.UTF_8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchRandomReadLatency {
    static final Logger LOG = LoggerFactory.getLogger(BenchRandomReadLatency.class);

    private static final Pattern LEDGER_PATTERN = Pattern.compile("L([0-9]+)$");

    private static final Comparator<String> ZK_LEDGER_COMPARE = new Comparator<String>() {
        public int compare(String o1, String o2) {
            try {
                Matcher m1 = LEDGER_PATTERN.matcher(o1);
                Matcher m2 = LEDGER_PATTERN.matcher(o2);
                if (m1.find() && m2.find()) {
                    return Integer.valueOf(m1.group(1))
                        - Integer.valueOf(m2.group(1));
                } else {
                    return o1.compareTo(o2);
                }
            } catch (Throwable t) {
                return o1.compareTo(o2);
            }
        }
    };

    private static void BenchRandomReadLatency(ClientConfiguration conf,
                                               ZooKeeper zk,
                                               int numReads,
                                               byte[] passwd) {
        BookKeeper bk = null;
        long bytesRead = 0;
        long time = 0;
        long entriesRead = 0;

        Random random = new Random(1214201518);
        byte[] data;
        LedgerHandle lh = null;

        try {
            // Get a list of all ledgers
            List<String> children = zk.getChildren("/ledgers", true);
            List<Long> ledgers = new ArrayList<Long>();
            for (String child : children) {
                final Matcher m = LEDGER_PATTERN.matcher(child);
                if (m.find()) {
                    ledgers.add(Long.valueOf(m.group(1)));
                }
            }

            if (ledgers.size() == 0) {
                LOG.info("There are no ledgers to read, exiting");
                System.exit(-1);
            }

            LOG.info("Number of ledgers found: " + ledgers.size());

            bk = new BookKeeper(conf);

            while (entriesRead < numReads) {
                int randId = random.nextInt(ledgers.size());
                long ledgerId = ledgers.get(randId);
                lh = bk.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32, 
                                             passwd);
                long lastConfirmed = lh.getLastAddConfirmed();
                // Skip this ledger if there are no entries.
                if (lastConfirmed == -1)
                    continue;

                long starttime = System.nanoTime();
                LOG.debug("Reading ledger {} {}", ledgerId, lastConfirmed);
                Enumeration<LedgerEntry> tail  = lh.readEntries(lastConfirmed, lastConfirmed);
                LedgerEntry e = tail.nextElement();
                data = e.getEntry();
                LOG.debug("ledger size {} {}", e.getEntryId(), data.length);
                bytesRead += data.length;
                entriesRead++;

                long entryId = random.nextLong() % lastConfirmed;
                if (entryId < 0)
                    entryId = -1 * entryId;
                LOG.debug("Reading ledger {} {}", ledgerId, entryId);
                Enumeration<LedgerEntry> ledgerEntry  = lh.readEntries(entryId, entryId);
                e = ledgerEntry.nextElement();
                data = e.getEntry();
                LOG.debug("ledger size {} {}", e.getEntryId(), data.length);
                bytesRead += data.length;
                entriesRead++;

                long endtime = System.nanoTime();
                time += endtime - starttime;

                if ((entriesRead % (numReads/10)) == 0) {
                    LOG.info("{} entries read", entriesRead);
                }

                lh.close();
                lh = null;
            }
        } catch (InterruptedException ie) {
            // ignore
        } catch (Exception e ) {
            LOG.error("Exception in random reader", e);
        } finally {
            LOG.info("Read {} in {}ms", entriesRead, time/1000/1000);
            LOG.info("Read {} bytes", bytesRead);

            try {
                if (lh != null) {
                    lh.close();
                }
                if (bk != null) {
                    bk.close();
                }
            } catch (Exception e) {
                LOG.error("Exception closing stuff", e);
            }
        }
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("BenchRandomReadLatency <options>", options);
    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("password", true, "Password used to access ledgers (default 'benchPasswd')");
        options.addOption("zookeeper", true, "Zookeeper ensemble, no default");
        options.addOption("sockettimeout", true, "Socket timeout for bookkeeper client. In seconds. Default 5");
        options.addOption("numreads", true, "Number of random reads to perform.  Default 10,000");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            usage(options);
            System.exit(-1);
        }

        final String servers = cmd.getOptionValue("zookeeper", "localhost:2181");
        final byte[] passwd = cmd.getOptionValue("password", "benchPasswd").getBytes(UTF_8);
        final int sockTimeout = Integer.valueOf(cmd.getOptionValue("sockettimeout", "5"));
        final int numReads = Integer.valueOf(cmd.getOptionValue("numreads", "10000"));

        final ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(sockTimeout).setZkServers(servers);

        LOG.info("(Parameters received) number of reads: " + numReads +
                 ", zk servers: " + servers); 

        final ZooKeeper zk = new ZooKeeper(servers, 3000, new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        LOG.info("Watcher started...");
                    }
                }});

        try {
            // Now do the benchmark
            BenchRandomReadLatency(conf, zk, numReads, passwd);
        } finally {
            zk.close();
        }
    }
}
