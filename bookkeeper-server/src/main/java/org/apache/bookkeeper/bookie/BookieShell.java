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

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.RoundingMode;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.Tool;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * Bookie Shell is to provide utilities for users to administer a bookkeeper cluster.
 */
public class BookieShell implements Tool {

    static final Logger LOG = LoggerFactory.getLogger(BookieShell.class);

    static final String CONF_OPT = "conf";
    static final String ENTRY_FORMATTER_OPT = "entryformat";
    static final String LEDGERID_FORMATTER_OPT = "ledgeridformat";

    static final String CMD_METAFORMAT = "metaformat";
    static final String CMD_INITNEWCLUSTER = "initnewcluster";
    static final String CMD_NUKEEXISTINGCLUSTER = "nukeexistingcluster";
    static final String CMD_INITBOOKIE = "initbookie";
    static final String CMD_BOOKIEFORMAT = "bookieformat";
    static final String CMD_RECOVER = "recover";
    static final String CMD_LEDGER = "ledger";
    static final String CMD_READ_LEDGER_ENTRIES = "readledger";
    static final String CMD_LISTLEDGERS = "listledgers";
    static final String CMD_LEDGERMETADATA = "ledgermetadata";
    static final String CMD_LISTUNDERREPLICATED = "listunderreplicated";
    static final String CMD_WHOISAUDITOR = "whoisauditor";
    static final String CMD_WHATISINSTANCEID = "whatisinstanceid";
    static final String CMD_SIMPLETEST = "simpletest";
    static final String CMD_BOOKIESANITYTEST = "bookiesanity";
    static final String CMD_READLOG = "readlog";
    static final String CMD_READLOGMETADATA = "readlogmetadata";
    static final String CMD_READJOURNAL = "readjournal";
    static final String CMD_LASTMARK = "lastmark";
    static final String CMD_AUTORECOVERY = "autorecovery";
    static final String CMD_LISTBOOKIES = "listbookies";
    static final String CMD_LISTFILESONDISC = "listfilesondisc";
    static final String CMD_UPDATECOOKIE = "updatecookie";
    static final String CMD_UPDATELEDGER = "updateledgers";
    static final String CMD_DELETELEDGER = "deleteledger";
    static final String CMD_BOOKIEINFO = "bookieinfo";
    static final String CMD_DECOMMISSIONBOOKIE = "decommissionbookie";
    static final String CMD_LOSTBOOKIERECOVERYDELAY = "lostbookierecoverydelay"; 
    static final String CMD_TRIGGERAUDIT = "triggeraudit";
    static final String CMD_HELP = "help";

    final ServerConfiguration bkConf = new ServerConfiguration();
    File[] indexDirectories;
    File[] ledgerDirectories;
    File journalDirectory;

    EntryLogger entryLogger = null;
    Journal journal = null;
    EntryFormatter entryFormatter;
    LedgerIdFormatter ledgerIdFormatter;

    int pageSize;
    int entriesPerPage;

    interface Command {
        public int runCmd(String[] args) throws Exception;
        public void printUsage();
    }

    abstract class MyCommand implements Command {
        abstract Options getOptions();
        abstract String getDescription();
        abstract String getUsage();
        abstract int runCmd(CommandLine cmdLine) throws Exception;

        String cmdName;

        MyCommand(String cmdName) {
            this.cmdName = cmdName;
        }

        @Override
        public int runCmd(String[] args) throws Exception {
            try {
                BasicParser parser = new BasicParser();
                CommandLine cmdLine = parser.parse(getOptions(), args);
                return runCmd(cmdLine);
            } catch (ParseException e) {
                LOG.error("Error parsing command line arguments : ", e);
                printUsage();
                return -1;
            }
        }

        @Override
        public void printUsage() {
            HelpFormatter hf = new HelpFormatter();
            LOG.error(cmdName + ": " + getDescription());
            hf.printHelp(getUsage(), getOptions());
        }
    }

    /**
     * Format the bookkeeper metadata present in zookeeper
     */
    class MetaFormatCmd extends MyCommand {
        Options opts = new Options();

        MetaFormatCmd() {
            super(CMD_METAFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format bookkeeper metadata in zookeeper";
        }

        @Override
        String getUsage() {
            return "metaformat   [-nonInteractive] [-force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            boolean result = BookKeeperAdmin.format(adminConf, interactive,
                    force);
            return (result) ? 0 : 1;
        }
    }
    
    /**
     * Intializes new cluster by creating required znodes for the cluster
     */
    class InitNewCluster extends MyCommand {
        Options opts = new Options();

        InitNewCluster() {
            super(CMD_INITNEWCLUSTER);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Initialize a new bookkeeper cluster";
        }

        @Override
        String getUsage() {
            return "initnewcluster";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            boolean result = BookKeeperAdmin.initNewCluster(adminConf);
            return (result) ? 0 : 1;
        }
    }
    
    /**
     * Nuke bookkeeper metadata of existing cluster in zookeeper
     */
    class NukeExistingCluster extends MyCommand {
        Options opts = new Options();

        NukeExistingCluster() {
            super(CMD_NUKEEXISTINGCLUSTER);
            opts.addOption("p", "zkledgersrootpath", true, "zookeeper ledgers rootpath");
            opts.addOption("i", "instanceid", true, "instanceid");
            opts.addOption("f", "force", false,
                    "If instanceid is not specified, then whether to force nuke the metadata without validating instanceid");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Nuke bookkeeper cluster by deleting metadata";
        }

        @Override
        String getUsage() {
            return "nukeexistingcluster -zkledgersrootpath <zkledgersrootpath> [-instanceid <instanceid> | -force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean force = cmdLine.hasOption("f");
            String zkledgersrootpath = cmdLine.getOptionValue("zkledgersrootpath");
            String instanceid = cmdLine.getOptionValue("instanceid");

            /*
             * for NukeExistingCluster command 'zkledgersrootpath' should be provided and either force option or
             * instanceid should be provided.
             */
            if ((zkledgersrootpath == null) || (force == (instanceid != null))) {
                LOG.error(
                        "zkledgersrootpath should be specified and either force option or instanceid should be specified (but not both)");
                printUsage();
                return -1;
            }

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            boolean result = BookKeeperAdmin.nukeExistingCluster(adminConf, zkledgersrootpath, instanceid, force);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Formats the local data present in current bookie server
     */
    class BookieFormatCmd extends MyCommand {
        Options opts = new Options();

        public BookieFormatCmd() {
            super(CMD_BOOKIEFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt..?");
            opts.addOption("d", "deleteCookie", false, "Delete its cookie on zookeeper");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format the current server contents";
        }

        @Override
        String getUsage() {
            return "bookieformat [-nonInteractive] [-force] [-deleteCookie]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ServerConfiguration conf = new ServerConfiguration(bkConf);
            boolean result = Bookie.format(conf, interactive, force);
            // delete cookie
            if (cmdLine.hasOption("d")) {
                ZooKeeperClient zkc =
                        ZooKeeperClient.newBuilder()
                                .connectString(conf.getZkServers())
                                .sessionTimeoutMs(conf.getZkTimeout())
                                .build();
                try {
                    Versioned<Cookie> cookie = Cookie.readFromZooKeeper(zkc, conf);
                    cookie.getValue().deleteFromZooKeeper(zkc, conf, cookie.getVersion());
                } catch (KeeperException.NoNodeException nne) {
                    LOG.warn("No cookie to remove : ", nne);
                } finally {
                    zkc.close();
                }
            }
            return (result) ? 0 : 1;
        }
    }

    /**
     * Initializes bookie, by making sure that the journalDir, ledgerDirs and
     * indexDirs are empty and there is no registered Bookie with this BookieId.
     */
    class InitBookieCmd extends MyCommand {
        Options opts = new Options();

        public InitBookieCmd() {
            super(CMD_INITBOOKIE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Initialize new Bookie";
        }

        @Override
        String getUsage() {
            return CMD_INITBOOKIE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            boolean result = BookKeeperAdmin.initBookie(conf);
            return (result) ? 0 : 1;
        }
    }
    
    /**
     * Recover command for ledger data recovery for failed bookie
     */
    class RecoverCmd extends MyCommand {
        Options opts = new Options();

        public RecoverCmd() {
            super(CMD_RECOVER);
            opts.addOption("d", "deleteCookie", false, "Delete cookie node for the bookie.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Recover the ledger data for failed bookie";
        }

        @Override
        String getUsage() {
            return "recover      [-deleteCookie] <bookieSrc> [bookieDest]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
            if (args.length < 1) {
                throw new MissingArgumentException(
                        "'bookieSrc' argument required");
            }

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                return bkRecovery(adminConf, admin, args, cmdLine.hasOption("d"));
            } finally {
                admin.close();
            }
        }

        private int bkRecovery(ClientConfiguration conf, BookKeeperAdmin bkAdmin,
                               String[] args, boolean deleteCookie)
                throws InterruptedException, BKException, KeeperException, IOException {
            final String bookieSrcString[] = args[0].split(":");
            if (bookieSrcString.length != 2) {
                LOG.error("BookieSrc inputted has invalid format"
                        + "(host:port expected): " + args[0]);
                return -1;
            }
            final BookieSocketAddress bookieSrc = new BookieSocketAddress(
                    bookieSrcString[0], Integer.parseInt(bookieSrcString[1]));
            BookieSocketAddress bookieDest = null;
            if (args.length >= 2) {
                final String bookieDestString[] = args[1].split(":");
                if (bookieDestString.length < 2) {
                    LOG.error("BookieDest inputted has invalid format"
                            + "(host:port expected): " + args[1]);
                    return -1;
                }
                bookieDest = new BookieSocketAddress(bookieDestString[0],
                        Integer.parseInt(bookieDestString[1]));
            }

            bkAdmin.recoverBookieData(bookieSrc, bookieDest);
            if (deleteCookie) {
                try {
                    Versioned<Cookie> cookie = Cookie.readFromZooKeeper(bkAdmin.getZooKeeper(), conf, bookieSrc);
                    cookie.getValue().deleteFromZooKeeper(bkAdmin.getZooKeeper(), conf, bookieSrc, cookie.getVersion());
                } catch (KeeperException.NoNodeException nne) {
                    LOG.warn("No cookie to remove for {} : ", bookieSrc, nne);
                }
            }
            return 0;
        }
    }

    /**
     * Ledger Command Handles ledger related operations
     */
    class LedgerCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerCmd() {
            super(CMD_LEDGER);
            lOpts.addOption("m", "meta", false, "Print meta information");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                LOG.error("ERROR: missing ledger id");
                printUsage();
                return -1;
            }

            boolean printMeta = false;
            if (cmdLine.hasOption("m")) {
                printMeta = true;
            }
            long ledgerId;
            try {
                ledgerId = ledgerIdFormatter.readLedgerId(leftArgs[0]);
            } catch (IllegalArgumentException iae) {
                LOG.error("ERROR: invalid ledger id " + leftArgs[0]);
                printUsage();
                return -1;
            }
            if (printMeta) {
                // print meta
                readLedgerMeta(ledgerId);
            }
            // dump ledger info
            readLedgerIndexEntries(ledgerId);
            return 0;
        }

        @Override
        String getDescription() {
            return "Dump ledger index entries into readable format.";
        }

        @Override
        String getUsage() {
            return "ledger       [-m] <ledger_id>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command for reading ledger entries
     */
    class ReadLedgerEntriesCmd extends MyCommand {
        Options lOpts = new Options();

        ReadLedgerEntriesCmd() {
            super(CMD_READ_LEDGER_ENTRIES);
            lOpts.addOption("m", "msg", false, "Print message body");
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
            lOpts.addOption("fe", "firstentryid", true, "First EntryID");
            lOpts.addOption("le", "lastentryid", true, "Last EntryID");
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        String getDescription() {
            return "Read a range of entries from a ledger";
        }

        @Override
        String getUsage() {
            return "readledger   [-msg] -ledgerid <ledgerid> [-firstentryid <firstentryid> [-lastentryid <lastentryid>]]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            final long ledgerId = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            if (ledgerId == -1) {
                LOG.error("Must specify a ledger id");
                return -1;
            }

            final long firstEntry = getOptionLongValue(cmdLine, "firstentryid", 0);
            final long lastEntry = getOptionLongValue(cmdLine, "lastentryid", -1);

            boolean printMsg = cmdLine.hasOption("m");

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);

            BookKeeperAdmin bk = null;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                bk = new BookKeeperAdmin(conf);
                Iterator<LedgerEntry> entries = bk.readEntries(ledgerId, firstEntry, lastEntry).iterator();
                while (entries.hasNext()) {
                    LedgerEntry entry = entries.next();
                    formatEntry(entry, printMsg);
                }
            } catch (Exception e) {
                LOG.error("Error reading entries from ledger {}", ledgerId, e.getCause());
                return -1;
            } finally {
                out.close();
                if (bk != null) {
                    bk.close();
                }
            }

            return 0;
        }

    }

    /**
     * Command for listing underreplicated ledgers
     */
    class ListUnderreplicatedCmd extends MyCommand {
        Options opts = new Options();

        public ListUnderreplicatedCmd() {
            super(CMD_LISTUNDERREPLICATED);
            opts.addOption("missingreplica", true, "Bookie Id of missing replica");
            opts.addOption("excludingmissingreplica", true, "Bookie Id of missing replica to ignore");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "List ledgers marked as underreplicated, with optional options to specify missingreplica (BookieId) and to exclude missingreplica";
        }

        @Override
        String getUsage() {
            return "listunderreplicated [[-missingreplica <bookieaddress>] [-excludingmissingreplica <bookieaddress>]]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {

            final String includingBookieId = cmdLine.getOptionValue("missingreplica");
            final String excludingBookieId = cmdLine.getOptionValue("excludingmissingreplica");

            Predicate<List<String>> predicate = null;
            if (!StringUtils.isBlank(includingBookieId) && !StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> (replicasList.contains(includingBookieId)
                        && !replicasList.contains(excludingBookieId));
            } else if (!StringUtils.isBlank(includingBookieId)) {
                predicate = replicasList -> replicasList.contains(includingBookieId);
            } else if (!StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> !replicasList.contains(excludingBookieId);
            }

            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                Iterator<Long> iter = underreplicationManager.listLedgersToRereplicate(predicate);
                while (iter.hasNext()) {
                    LOG.info(ledgerIdFormatter.formatLedgerId(iter.next()));
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    final static int LIST_BATCH_SIZE = 1000;
    /**
     * Command to list all ledgers in the cluster
     */
    class ListLedgersCmd extends MyCommand {
        Options lOpts = new Options();

        ListLedgersCmd() {
            super(CMD_LISTLEDGERS);
            lOpts.addOption("m", "meta", false, "Print metadata");

        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            LedgerManagerFactory mFactory = null;
            LedgerManager m = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                m = mFactory.newLedgerManager();
                LedgerRangeIterator iter = m.getLedgerRanges();
                if (cmdLine.hasOption("m")) {
                    List<ReadMetadataCallback> futures
                        = new ArrayList<ReadMetadataCallback>(LIST_BATCH_SIZE);
                    while (iter.hasNext()) {
                        LedgerRange r = iter.next();
                        for (Long lid : r.getLedgers()) {
                            ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                            m.readLedgerMetadata(lid, cb);
                            futures.add(cb);
                        }
                        if (futures.size() >= LIST_BATCH_SIZE) {
                            while (futures.size() > 0) {
                                ReadMetadataCallback cb = futures.remove(0);
                                printLedgerMetadata(cb);
                            }
                        }
                    }
                    while (futures.size() > 0) {
                        ReadMetadataCallback cb = futures.remove(0);
                        printLedgerMetadata(cb);
                    }
                } else {
                    while (iter.hasNext()) {
                        LedgerRange r = iter.next();
                        for (Long lid : r.getLedgers()) {
                            LOG.info(ledgerIdFormatter.formatLedgerId(lid));
                        }
                    }
                }
            } finally {
                if (m != null) {
                    try {
                      m.close();
                      mFactory.uninitialize();
                    } catch (IOException ioe) {
                      LOG.error("Failed to close ledger manager : ", ioe);
                    }
                }
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "List all ledgers on the cluster (this may take a long time)";
        }

        @Override
        String getUsage() {
            return "listledgers  [-meta]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    void printLedgerMetadata(ReadMetadataCallback cb) throws Exception {
        LedgerMetadata md = cb.get();
        LOG.info("ledgerID: " + ledgerIdFormatter.formatLedgerId(cb.getLedgerId()));
        LOG.info(new String(md.serialize(), UTF_8));
    }

    static class ReadMetadataCallback extends AbstractFuture<LedgerMetadata>
        implements GenericCallback<LedgerMetadata> {
        final long ledgerId;

        ReadMetadataCallback(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        long getLedgerId() {
            return ledgerId;
        }

        public void operationComplete(int rc, LedgerMetadata result) {
            if (rc != 0) {
                setException(BKException.create(rc));
            } else {
                set(result);
            }
        }
    }

    /**
     * Print the metadata for a ledger
     */
    class LedgerMetadataCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerMetadataCmd() {
            super(CMD_LEDGERMETADATA);
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final long lid = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            if (lid == -1) {
                LOG.error("Must specify a ledger id");
                return -1;
            }

            ZooKeeper zk = null;
            LedgerManagerFactory mFactory = null;
            LedgerManager m = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                m = mFactory.newLedgerManager();
                ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                m.readLedgerMetadata(lid, cb);
                printLedgerMetadata(cb);
            } finally {
                if (m != null) {
                    try {
                        m.close();
                        mFactory.uninitialize();
                    } catch (IOException ioe) {
                        LOG.error("Failed to close ledger manager : ", ioe);
                    }
                }
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "Print the metadata for a ledger";
        }

        @Override
        String getUsage() {
            return "ledgermetadata -ledgerid <ledgerid>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Simple test to create a ledger and write to it
     */
    class SimpleTestCmd extends MyCommand {
        Options lOpts = new Options();

        SimpleTestCmd() {
            super(CMD_SIMPLETEST);
            lOpts.addOption("e", "ensemble", true, "Ensemble size (default 3)");
            lOpts.addOption("w", "writeQuorum", true, "Write quorum size (default 2)");
            lOpts.addOption("a", "ackQuorum", true, "Ack quorum size (default 2)");
            lOpts.addOption("n", "numEntries", true, "Entries to write (default 1000)");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            byte[] data = new byte[100]; // test data

            int ensemble = getOptionIntValue(cmdLine, "ensemble", 3);
            int writeQuorum = getOptionIntValue(cmdLine, "writeQuorum", 2);
            int ackQuorum = getOptionIntValue(cmdLine, "ackQuorum", 2);
            int numEntries = getOptionIntValue(cmdLine, "numEntries", 1000);

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            BookKeeper bk = new BookKeeper(conf);
            LedgerHandle lh = bk.createLedger(ensemble, writeQuorum, ackQuorum,
                                              BookKeeper.DigestType.MAC, new byte[0]);
            LOG.info("Ledger ID: " + lh.getId());
            long lastReport = System.nanoTime();
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(data);
                if (TimeUnit.SECONDS.convert(System.nanoTime() - lastReport,
                                             TimeUnit.NANOSECONDS) > 1) {
                    LOG.info(i + " entries written");
                    lastReport = System.nanoTime();
                }
            }

            lh.close();
            bk.close();
            LOG.info(numEntries + " entries written to ledger " + lh.getId());

            return 0;
        }

        @Override
        String getDescription() {
            return "Simple test to create a ledger and write entries to it";
        }

        @Override
        String getUsage() {
            return "simpletest   [-ensemble N] [-writeQuorum N] [-ackQuorum N] [-numEntries N]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command to run a bookie sanity test
     */
    class BookieSanityTestCmd extends MyCommand {
        Options lOpts = new Options();

        BookieSanityTestCmd() {
            super(CMD_BOOKIESANITYTEST);
            lOpts.addOption("e", "entries", true, "Total entries to be added for the test (default 10)");
            lOpts.addOption("t", "timeout", true, "Timeout for write/read operations in seconds (default 1)");
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        String getDescription() {
            return "Sanity test for local bookie. Create ledger and write/reads entries on local bookie.";
        }

        @Override
        String getUsage() {
            return "bookiesanity [-entries N] [-timeout N]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            int numberOfEntries = getOptionIntValue(cmdLine, "entries", 10);
            int timeoutSecs= getOptionIntValue(cmdLine, "timeout", 1);

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            conf.setEnsemblePlacementPolicy(LocalBookieEnsemblePlacementPolicy.class);
            conf.setAddEntryTimeout(timeoutSecs);
            conf.setReadEntryTimeout(timeoutSecs);

            BookKeeper bk = new BookKeeper(conf);
            LedgerHandle lh = null;
            try {
                lh = bk.createLedger(1, 1, DigestType.MAC, new byte[0]);
                LOG.info("Created ledger {}", lh.getId());

                for (int i = 0; i < numberOfEntries; i++) {
                    String content = "entry-" + i;
                    lh.addEntry(content.getBytes(UTF_8));
                }

                LOG.info("Written {} entries in ledger {}", numberOfEntries, lh.getId());

                // Reopen the ledger and read entries
                lh = bk.openLedger(lh.getId(), DigestType.MAC, new byte[0]);
                if (lh.getLastAddConfirmed() != (numberOfEntries - 1)) {
                    throw new Exception("Invalid last entry found on ledger. expecting: " + (numberOfEntries - 1)
                            + " -- found: " + lh.getLastAddConfirmed());
                }

                Enumeration<LedgerEntry> entries = lh.readEntries(0, numberOfEntries - 1);
                int i = 0;
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    String actualMsg = new String(entry.getEntry(), UTF_8);
                    String expectedMsg = "entry-" + (i++);
                    if (!expectedMsg.equals(actualMsg)) {
                        throw new Exception("Failed validation of received message - Expected: " + expectedMsg
                                + ", Actual: " + actualMsg);
                    }
                }

                LOG.info("Read {} entries from ledger {}", entries, lh.getId());
            } catch (Exception e) {
                LOG.warn("Error in bookie sanity test", e);
                return -1;
            } finally {
                if (lh != null) {
                    bk.deleteLedger(lh.getId());
                    LOG.info("Deleted ledger {}", lh.getId());
                }

                bk.close();
            }

            LOG.info("Bookie sanity test succeeded");
            return 0;
        }
    }

    /**
     * Command to read entry log files.
     */
    class ReadLogCmd extends MyCommand {
        Options rlOpts = new Options();

        ReadLogCmd() {
            super(CMD_READLOG);
            rlOpts.addOption("m", "msg", false, "Print message body");
            rlOpts.addOption("l", "ledgerid", true, "Ledger ID");
            rlOpts.addOption("e", "entryid", true, "EntryID");
            rlOpts.addOption("sp", "startpos", true, "Start Position");
            rlOpts.addOption("ep", "endpos", true, "End Position");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                LOG.error("ERROR: missing entry log id or entry log file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long logId;
            try {
                logId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a entry log id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".log")) {
                    // not a log file
                    LOG.error("ERROR: invalid entry log file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                logId = Long.parseLong(idString, 16);
            }

            final long lId = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            final long eId = getOptionLongValue(cmdLine, "entryid", -1);
            final long startpos = getOptionLongValue(cmdLine, "startpos", -1);
            final long endpos = getOptionLongValue(cmdLine, "endpos", -1);

            // scan entry log
            if (startpos != -1) {
                if ((endpos != -1) && (endpos < startpos)) {
                    LOG.error("ERROR: StartPosition of the range should be lesser than or equal to EndPosition");
                    return -1;
                }
                scanEntryLogForPositionRange(logId, startpos, endpos, printMsg);
            } else if (lId != -1) {
                scanEntryLogForSpecificEntry(logId, lId, eId, printMsg);
            } else {
                scanEntryLog(logId, printMsg);
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "Scan an entry file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readlog      [-msg] <entry_log_id | entry_log_file_name> [-ledgerid <ledgerid> [-entryid <entryid>]] "
                    + "[-startpos <startEntryLogBytePos> [-endpos <endEntryLogBytePos>]]";
        }

        @Override
        Options getOptions() {
            return rlOpts;
        }
    }

    /**
     * Command to print metadata of entrylog.
     */
    class ReadLogMetadataCmd extends MyCommand {
        Options rlOpts = new Options();

        ReadLogMetadataCmd() {
            super(CMD_READLOGMETADATA);
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                LOG.error("ERROR: missing entry log id or entry log file name");
                printUsage();
                return -1;
            }

            long logId;
            try {
                logId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a entry log id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".log")) {
                    // not a log file
                    LOG.error("ERROR: invalid entry log file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                logId = Long.parseLong(idString, 16);
            }

            printEntryLogMetadata(logId);

            return 0;
        }

        @Override
        String getDescription() {
            return "Prints entrylog's metadata";
        }

        @Override
        String getUsage() {
            return "readlogmetadata <entry_log_id | entry_log_file_name>";
        }

        @Override
        Options getOptions() {
            return rlOpts;
        }
    }
    
    /**
     * Command to read journal files
     */
    class ReadJournalCmd extends MyCommand {
        Options rjOpts = new Options();

        ReadJournalCmd() {
            super(CMD_READJOURNAL);
            rjOpts.addOption("m", "msg", false, "Print message body");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                LOG.error("ERROR: missing journal id or journal file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long journalId;
            try {
                journalId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a journal id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".txn")) {
                    // not a journal file
                    LOG.error("ERROR: invalid journal file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                journalId = Long.parseLong(idString, 16);
            }
            // scan journal
            scanJournal(journalId, printMsg);
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan a journal file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readjournal  [-msg] <journal_id | journal_file_name>";
        }

        @Override
        Options getOptions() {
            return rjOpts;
        }
    }

    /**
     * Command to print last log mark
     */
    class LastMarkCmd extends MyCommand {
        LastMarkCmd() {
            super(CMD_LASTMARK);
        }

        @Override
        public int runCmd(CommandLine c) throws Exception {
            printLastLogMark();
            return 0;
        }

        @Override
        String getDescription() {
            return "Print last log marker.";
        }

        @Override
        String getUsage() {
            return "lastmark";
        }

        @Override
        Options getOptions() {
            return new Options();
        }
    }

    /**
     * List available bookies
     */
    class ListBookiesCmd extends MyCommand {
        Options opts = new Options();

        ListBookiesCmd() {
            super(CMD_LISTBOOKIES);
            opts.addOption("rw", "readwrite", false, "Print readwrite bookies");
            opts.addOption("ro", "readonly", false, "Print readonly bookies");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            boolean readwrite = cmdLine.hasOption("rw");
            boolean readonly = cmdLine.hasOption("ro");

            if ((!readwrite && !readonly) || (readwrite && readonly)) {
                LOG.error("One and only one of -readwrite and -readonly must be specified");
                printUsage();
                return 1;
            }
            ClientConfiguration clientconf = new ClientConfiguration(bkConf)
                .setZkServers(bkConf.getZkServers());
            BookKeeperAdmin bka = new BookKeeperAdmin(clientconf);

            int count = 0;
            Collection<BookieSocketAddress> bookies = new ArrayList<BookieSocketAddress>();
            if (cmdLine.hasOption("rw")) {
                Collection<BookieSocketAddress> availableBookies = bka
                        .getAvailableBookies();
                bookies.addAll(availableBookies);
            } else if (cmdLine.hasOption("ro")) {
                Collection<BookieSocketAddress> roBookies = bka
                        .getReadOnlyBookies();
                bookies.addAll(roBookies);
            }
            for (BookieSocketAddress b : bookies) {
                LOG.info(getBookieSocketAddrStringRepresentation(b));
                count++;
            }
            if (count == 0) {
                LOG.error("No bookie exists!");
                return 1;
            }
            return 0;
        }

        @Override
        String getDescription() {
            return "List the bookies, which are running as either readwrite or readonly mode.";
        }

        @Override
        String getUsage() {
            return "listbookies  [-readwrite|-readonly] [-hostnames]";
        }

        @Override
        Options getOptions() {
            return opts;
        }
    }

    class ListDiskFilesCmd extends MyCommand {
        Options opts = new Options();

        ListDiskFilesCmd() {
            super(CMD_LISTFILESONDISC);
            opts.addOption("txn", "journal", false, "Print list of Journal Files");
            opts.addOption("log", "entrylog", false, "Print list of EntryLog Files");
            opts.addOption("idx", "index", false, "Print list of Index files");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {

            boolean journal = cmdLine.hasOption("txn");
            boolean entrylog = cmdLine.hasOption("log");
            boolean index = cmdLine.hasOption("idx");
            boolean all = false;

            if (!journal && !entrylog && !index && !all) {
                all = true;
            }

            if (all || journal) {
                File journalDir = bkConf.getJournalDir();
                List<File> journalFiles = listFilesAndSort(new File[] { journalDir }, "txn");
                LOG.info("--------- Printing the list of Journal Files ---------");
                for (File journalFile : journalFiles) {
                    LOG.info(journalFile.getCanonicalPath());
                }                
                LOG.info("");
            }
            if (all || entrylog) {
                File[] ledgerDirs = bkConf.getLedgerDirs();
                List<File> ledgerFiles = listFilesAndSort(ledgerDirs, "log");
                LOG.info("--------- Printing the list of EntryLog/Ledger Files ---------");
                for (File ledgerFile : ledgerFiles) {
                    LOG.info(ledgerFile.getCanonicalPath());
                }
                LOG.info("");
            }
            if (all || index) {
                File[] indexDirs = (bkConf.getIndexDirs() == null) ? bkConf.getLedgerDirs() : bkConf.getIndexDirs();
                List<File> indexFiles = listFilesAndSort(indexDirs, "idx");
                LOG.info("--------- Printing the list of Index Files ---------");
                for (File indexFile : indexFiles) {
                    LOG.info(indexFile.getCanonicalPath());
                }
            }
            return 0;
        }

        @Override
        String getDescription() {
            return "List the files in JournalDirectory/LedgerDirectories/IndexDirectories";
        }

        @Override
        String getUsage() {
            return "listfilesondisc  [-journal|-entrylog|-index]";
        }

        @Override
        Options getOptions() {
            return opts;
        }
    }


    /**
     * Command to print help message
     */
    class HelpCmd extends MyCommand {
        HelpCmd() {
            super(CMD_HELP);
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
            if (args.length == 0) {
                printShellUsage();
                return 0;
            }
            String cmdName = args[0];
            Command cmd = commands.get(cmdName);
            if (null == cmd) {
                LOG.error("Unknown command " + cmdName);
                printShellUsage();
                return -1;
            }
            cmd.printUsage();
            return 0;
        }

        @Override
        String getDescription() {
            return "Describe the usage of this program or its subcommands.";
        }

        @Override
        String getUsage() {
            return "help         [COMMAND]";
        }

        @Override
        Options getOptions() {
            return new Options();
        }
    }

    /**
     * Command for administration of autorecovery
     */
    class AutoRecoveryCmd extends MyCommand {
        Options opts = new Options();

        public AutoRecoveryCmd() {
            super(CMD_AUTORECOVERY);
            opts.addOption("e", "enable", false,
                           "Enable auto recovery of underreplicated ledgers");
            opts.addOption("d", "disable", false,
                           "Disable auto recovery of underreplicated ledgers");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Enable or disable autorecovery in the cluster.";
        }

        @Override
        String getUsage() {
            return "autorecovery [-enable|-disable]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean disable = cmdLine.hasOption("d");
            boolean enable = cmdLine.hasOption("e");

            if ((!disable && !enable)
                || (enable && disable)) {
                LOG.error("One and only one of -enable and -disable must be specified");
                printUsage();
                return 1;
            }
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                if (enable) {
                    if (underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already enabled. Doing nothing");
                    } else {
                        LOG.info("Enabling autorecovery");
                        underreplicationManager.enableLedgerReplication();
                    }
                } else {
                    if (!underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already disabled. Doing nothing");
                    } else {
                        LOG.info("Disabling autorecovery");
                        underreplicationManager.disableLedgerReplication();
                    }
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    /**
     * Setter and Getter for LostBookieRecoveryDelay value (in seconds) in Zookeeper
     */
    class LostBookieRecoveryDelayCmd extends MyCommand {
        Options opts = new Options();

        public LostBookieRecoveryDelayCmd() {
            super(CMD_LOSTBOOKIERECOVERYDELAY);
            opts.addOption("g", "get", false, "Get LostBookieRecoveryDelay value (in seconds)");
            opts.addOption("s", "set", true, "Set LostBookieRecoveryDelay value (in seconds)");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Setter and Getter for LostBookieRecoveryDelay value (in seconds) in Zookeeper";
        }

        @Override
        String getUsage() {
            return "lostbookierecoverydelay [-get|-set <value>]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean getter = cmdLine.hasOption("g");
            boolean setter = cmdLine.hasOption("s");

            if ((!getter && !setter) || (getter && setter)) {
                LOG.error("One and only one of -get and -set must be specified");
                printUsage();
                return 1;
            }
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                if (getter) {
                    int lostBookieRecoveryDelay = admin.getLostBookieRecoveryDelay();
                    LOG.info("LostBookieRecoveryDelay value in ZK: {}", String.valueOf(lostBookieRecoveryDelay));
                } else {
                    int lostBookieRecoveryDelay = Integer.parseInt(cmdLine.getOptionValue("set"));
                    admin.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);
                    LOG.info("Successfully set LostBookieRecoveryDelay value in ZK: {}",
                            String.valueOf(lostBookieRecoveryDelay));
                }
            } finally {
                if (admin != null) {
                    admin.close();
                }
            }
            return 0;
        }
    }
    
    
    /**
     * Print which node has the auditor lock
     */
    class WhoIsAuditorCmd extends MyCommand {
        Options opts = new Options();

        public WhoIsAuditorCmd() {
            super(CMD_WHOISAUDITOR);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Print the node which holds the auditor lock";
        }

        @Override
        String getUsage() {
            return "whoisauditor";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                BookieSocketAddress bookieId = AuditorElector.getCurrentAuditor(bkConf, zk);
                if (bookieId == null) {
                    LOG.info("No auditor elected");
                    return -1;
                }
                LOG.info("Auditor: " + getBookieSocketAddrStringRepresentation(bookieId));
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    /**
     * Prints the instanceid of the cluster
     */
    class WhatIsInstanceId extends MyCommand {
        Options opts = new Options();

        public WhatIsInstanceId() {
            super(CMD_WHATISINSTANCEID);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Print the instanceid of the cluster";
        }

        @Override
        String getUsage() {
            return "whatisinstanceid";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder().connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout()).build();
                byte[] data = zk.getData(bkConf.getZkLedgersRootPath() + "/" + BookKeeperConstants.INSTANCEID, false,
                        null);
                String readInstanceId = new String(data, UTF_8);
                LOG.info("ZKServers: {} ZkLedgersRootPath: {} InstanceId: {}", bkConf.getZkServers(),
                        bkConf.getZkLedgersRootPath(), readInstanceId);
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }
    
    /**
     * Update cookie command
     */
    class UpdateCookieCmd extends MyCommand {
        Options opts = new Options();
        private static final String BOOKIEID = "bookieId";
        private static final String EXPANDSTORAGE = "expandstorage";
        private static final String LIST = "list";
        private static final String DELETE = "delete";
        private static final String HOSTNAME = "hostname";
        private static final String IP = "ip";
        private static final String FORCE = "force";

        UpdateCookieCmd() {
            super(CMD_UPDATECOOKIE);
            opts.addOption("b", BOOKIEID, true, "Bookie Id");
            opts.addOption("e", EXPANDSTORAGE, false, "Expand Storage");
            opts.addOption("l", LIST, false, "List paths of all the cookies present locally and on zookkeeper");
            @SuppressWarnings("static-access")
            Option deleteOption = OptionBuilder.withLongOpt(DELETE).hasOptionalArgs(1)
                    .withDescription("Delete cookie both locally and in ZooKeeper").create("d");
            opts.addOption(deleteOption);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Command to update cookie"
                    + "bookieId - Update bookie id in cookie\n"
                    + "expandstorage - Add new empty ledger/index directories. Update the directories info in the conf file before running the command\n"
                    + "list - list the local cookie files path and ZK cookiePath "
                    + "delete - Delete cookies locally and in zookeeper";
        }

        @Override
        String getUsage() {
            return "updatecookie [-bookieId <hostname|ip>] [-expandstorage] [-list] [-delete <force>]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            int retValue = -1;
            Option[] options = cmdLine.getOptions();
            if (options.length != 1) {
                LOG.error("Invalid command!");
                this.printUsage();
                return -1;
            }
            Option thisCommandOption = options[0];
            if (thisCommandOption.getLongOpt().equals(BOOKIEID)) {
                final String bookieId = cmdLine.getOptionValue(BOOKIEID);
                if (StringUtils.isBlank(bookieId)) {
                    LOG.error("Invalid argument list!");
                    this.printUsage();
                    return -1;
                }
                if (!StringUtils.equals(bookieId, HOSTNAME) && !StringUtils.equals(bookieId, IP)) {
                    LOG.error("Invalid option value:" + bookieId);
                    this.printUsage();
                    return -1;
                }
                boolean useHostName = getOptionalValue(bookieId, HOSTNAME);
                if (!bkConf.getUseHostNameAsBookieID() && useHostName) {
                    LOG.error(
                            "Expects configuration useHostNameAsBookieID=true as the option value passed is 'hostname'");
                    return -1;
                } else if (bkConf.getUseHostNameAsBookieID() && !useHostName) {
                    LOG.error("Expects configuration useHostNameAsBookieID=false as the option value passed is 'ip'");
                    return -1;
                }
                retValue = updateBookieIdInCookie(bookieId, useHostName);
            } else if (thisCommandOption.getLongOpt().equals(EXPANDSTORAGE)) {
                return expandStorage();
            } else if (thisCommandOption.getLongOpt().equals(LIST)) {
                return listOrDeleteCookies(false, false);
            } else if (thisCommandOption.getLongOpt().equals(DELETE)) {
                boolean force = false;
                String optionValue = thisCommandOption.getValue();
                if (!StringUtils.isEmpty(optionValue) && optionValue.equals(FORCE)) {
                    force = true;
                }
                return listOrDeleteCookies(true, force);
            } else {
                LOG.error("Invalid command!");
                this.printUsage();
                return -1;
            }
            return retValue;
        }

        private int updateBookieIdInCookie(final String bookieId, final boolean useHostname)
                throws IOException, InterruptedException {
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder().connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout()).build();
                ServerConfiguration conf = new ServerConfiguration(bkConf);
                String newBookieId = Bookie.getBookieAddress(conf).toString();
                // read oldcookie
                Versioned<Cookie> oldCookie = null;
                try {
                    conf.setUseHostNameAsBookieID(!useHostname);
                    oldCookie = Cookie.readFromZooKeeper(zk, conf);
                } catch (KeeperException.NoNodeException nne) {
                    LOG.error("Either cookie already updated with UseHostNameAsBookieID={} or no cookie exists!",
                            useHostname, nne);
                    return -1;
                }
                Cookie newCookie = Cookie.newBuilder(oldCookie.getValue()).setBookieHost(newBookieId).build();
                boolean hasCookieUpdatedInDirs = verifyCookie(newCookie, journalDirectory);
                for (File dir : ledgerDirectories) {
                    hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                }
                if (indexDirectories != ledgerDirectories) {
                    for (File dir : indexDirectories) {
                        hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                    }
                }

                if (hasCookieUpdatedInDirs) {
                    try {
                        conf.setUseHostNameAsBookieID(useHostname);
                        Cookie.readFromZooKeeper(zk, conf);
                        // since newcookie exists, just do cleanup of oldcookie
                        // and return
                        conf.setUseHostNameAsBookieID(!useHostname);
                        oldCookie.getValue().deleteFromZooKeeper(zk, conf, oldCookie.getVersion());
                        return 0;
                    } catch (KeeperException.NoNodeException nne) {
                        LOG.debug("Ignoring, cookie will be written to zookeeper");
                    }
                } else {
                    // writes newcookie to local dirs
                    newCookie.writeToDirectory(journalDirectory);
                    LOG.info("Updated cookie file present in journalDirectory {}", journalDirectory);
                    for (File dir : ledgerDirectories) {
                        newCookie.writeToDirectory(dir);
                    }
                    LOG.info("Updated cookie file present in ledgerDirectories {}", Arrays.toString(ledgerDirectories));
                    if (ledgerDirectories != indexDirectories) {
                        for (File dir : indexDirectories) {
                            newCookie.writeToDirectory(dir);
                        }
                        LOG.info("Updated cookie file present in indexDirectories {}",
                                Arrays.toString(indexDirectories));
                    }
                }
                // writes newcookie to zookeeper
                conf.setUseHostNameAsBookieID(useHostname);
                newCookie.writeToZooKeeper(zk, conf, Version.NEW);

                // delete oldcookie
                conf.setUseHostNameAsBookieID(!useHostname);
                oldCookie.getValue().deleteFromZooKeeper(zk, conf, oldCookie.getVersion());
            } catch (KeeperException ke) {
                LOG.error("KeeperException during cookie updation!", ke);
                return -1;
            } catch (IOException ioe) {
                LOG.error("IOException during cookie updation!", ioe);
                return -1;
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
            return 0;
        }

        private int expandStorage() throws InterruptedException {
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder().connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout()).build();

                List<File> allLedgerDirs = Lists.newArrayList();
                allLedgerDirs.addAll(Arrays.asList(ledgerDirectories));
                if (indexDirectories != ledgerDirectories) {
                    allLedgerDirs.addAll(Arrays.asList(indexDirectories));
                }

                try {
                    Bookie.checkEnvironmentWithStorageExpansion(bkConf, zk, journalDirectory, allLedgerDirs);
                } catch (BookieException | IOException e) {
                    LOG.error("Exception while updating cookie for storage expansion", e);
                    return -1;
                }
            } catch (KeeperException | InterruptedException | IOException e) {
                LOG.error("Exception while establishing zookeeper connection.", e);
                return -1;
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
            return 0;
        }

        private boolean verifyCookie(Cookie oldCookie, File dir) throws IOException {
            try {
                Cookie cookie = Cookie.readFromDirectory(dir);
                cookie.verify(oldCookie);
            } catch (InvalidCookieException e) {
                return false;
            }
            return true;
        }

        private int listOrDeleteCookies(boolean delete, boolean force)
                throws IOException, KeeperException, InterruptedException {
            BookieSocketAddress bookieAddress = Bookie.getBookieAddress(bkConf);
            File journalDir = bkConf.getJournalDir();
            File[] ledgerDirs = bkConf.getLedgerDirs();
            File[] indexDirs = bkConf.getIndexDirs();
            File[] allDirs = ArrayUtils.addAll(new File[] { journalDir }, ledgerDirs);
            if (indexDirs != null) {
                allDirs = ArrayUtils.addAll(allDirs, indexDirs);
            }

            File[] allCurDirs = Bookie.getCurrentDirectories(allDirs);
            List<File> allVersionFiles = new LinkedList<File>();
            File versionFile;
            for (File curDir : allCurDirs) {
                versionFile = new File(curDir, BookKeeperConstants.VERSION_FILENAME);
                if (versionFile.exists()) {
                    allVersionFiles.add(versionFile);
                }
            }

            if (!allVersionFiles.isEmpty()) {
                if (delete) {
                    boolean confirm = force;
                    if (!confirm) {
                        confirm = IOUtils.confirmPrompt("Are you sure you want to delete Cookies locally?");
                    }
                    if (confirm) {
                        for (File verFile : allVersionFiles) {
                            if (!verFile.delete()) {
                                LOG.error(
                                        "Failed to delete Local cookie file {}. So aborting deletecookie of Bookie: {}",
                                        verFile, bookieAddress);
                                return -1;
                            }
                        }
                        LOG.info("Deleted Local Cookies of Bookie: {}", bookieAddress);
                    } else {
                        LOG.info("Skipping deleting local Cookies of Bookie: {}", bookieAddress);
                    }
                } else {
                    LOG.info("Listing local Cookie Files of Bookie: {}", bookieAddress);
                    for (File verFile : allVersionFiles) {
                        LOG.info(verFile.getCanonicalPath());
                    }
                }
            } else {
                LOG.info("No local cookies for Bookie: {}", bookieAddress);
            }

            ZooKeeper zk = null;
            try {                
                Versioned<Cookie> cookie = null;
                try {
                    zk = ZooKeeperClient.newBuilder().connectString(bkConf.getZkServers())
                            .sessionTimeoutMs(bkConf.getZkTimeout()).build();                    
                    cookie = Cookie.readFromZooKeeper(zk, bkConf, bookieAddress);
                } catch (KeeperException.NoNodeException nne) {
                    LOG.info("No cookie for {} in ZooKeeper", bookieAddress);
                    return 0;
                }

                if (delete) {
                    boolean confirm = force;
                    if (!confirm) {
                        confirm = IOUtils.confirmPrompt("Are you sure you want to delete Cookies from Zookeeper?");
                    }

                    if (confirm) {
                        cookie.getValue().deleteFromZooKeeper(zk, bkConf, bookieAddress, cookie.getVersion());
                        LOG.info("Deleted Cookie from Zookeeper for Bookie: {}", bookieAddress);
                    } else {
                        LOG.info("Skipping deleting cookie from ZooKeeper for Bookie: {}", bookieAddress);
                    }
                } else {
                    LOG.info("{} bookie's Cookie path in Zookeeper: {} ", bookieAddress, Cookie.getZkPath(bkConf));
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
            return 0;
        }
    }

    /**
     * Update ledger command
     */
    class UpdateLedgerCmd extends MyCommand {
        private final Options opts = new Options();

        UpdateLedgerCmd() {
            super(CMD_UPDATELEDGER);
            opts.addOption("b", "bookieId", true, "Bookie Id");
            opts.addOption("s", "updatespersec", true, "Number of ledgers updating per second (default: 5 per sec)");
            opts.addOption("l", "limit", true, "Maximum number of ledgers to update (default: no limit)");
            opts.addOption("v", "verbose", true, "Print status of the ledger updation (default: false)");
            opts.addOption("p", "printprogress", true,
                    "Print messages on every configured seconds if verbose turned on (default: 10 secs)");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Update bookie id in ledgers (this may take a long time)";
        }

        @Override
        String getUsage() {
            return "updateledgers -bookieId <hostname|ip> [-updatespersec N] [-limit N] [-verbose true/false] [-printprogress N]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            final String bookieId = cmdLine.getOptionValue("bookieId");
            if (StringUtils.isBlank(bookieId)) {
                LOG.error("Invalid argument list!");
                this.printUsage();
                return -1;
            }
            if (!StringUtils.equals(bookieId, "hostname") && !StringUtils.equals(bookieId, "ip")) {
                LOG.error("Invalid option value {} for bookieId, expected hostname/ip", bookieId);
                this.printUsage();
                return -1;
            }
            boolean useHostName = getOptionalValue(bookieId, "hostname");
            if (!bkConf.getUseHostNameAsBookieID() && useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=true as the option value passed is 'hostname'");
                return -1;
            } else if (bkConf.getUseHostNameAsBookieID() && !useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=false as the option value passed is 'ip'");
                return -1;
            }
            final int rate = getOptionIntValue(cmdLine, "updatespersec", 5);
            if (rate <= 0) {
                LOG.error("Invalid updatespersec {}, should be > 0", rate);
                return -1;
            }
            final int limit = getOptionIntValue(cmdLine, "limit", Integer.MIN_VALUE);
            if (limit <= 0 && limit != Integer.MIN_VALUE) {
                LOG.error("Invalid limit {}, should be > 0", limit);
                return -1;
            }
            final boolean verbose = getOptionBooleanValue(cmdLine, "verbose", false);
            final long printprogress;
            if (!verbose) {
                if (cmdLine.hasOption("printprogress")) {
                    LOG.warn("Ignoring option 'printprogress', this is applicable when 'verbose' is true");
                }
                printprogress = Integer.MIN_VALUE;
            } else {
                // defaulting to 10 seconds
                printprogress = getOptionLongValue(cmdLine, "printprogress", 10);
            }
            final ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            final BookKeeper bk = new BookKeeper(conf);
            final BookKeeperAdmin admin = new BookKeeperAdmin(conf);
            final UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, admin);
            final ServerConfiguration serverConf = new ServerConfiguration(bkConf);
            final BookieSocketAddress newBookieId = Bookie.getBookieAddress(serverConf);
            serverConf.setUseHostNameAsBookieID(!useHostName);
            final BookieSocketAddress oldBookieId = Bookie.getBookieAddress(serverConf);

            UpdateLedgerNotifier progressable = new UpdateLedgerNotifier() {
                long lastReport = System.nanoTime();

                @Override
                public void progress(long updated, long issued) {
                    if (printprogress <= 0) {
                        return; // disabled
                    }
                    if (TimeUnit.MILLISECONDS.toSeconds(MathUtils.elapsedMSec(lastReport)) >= printprogress) {
                        LOG.info("Number of ledgers issued={}, updated={}", issued, updated);
                        lastReport = MathUtils.nowInNano();
                    }
                }
            };
            try {
                updateLedgerOp.updateBookieIdInLedgers(oldBookieId, newBookieId, rate, limit, progressable);
            } catch (BKException | IOException e) {
                LOG.error("Failed to update ledger metadata", e);
                return -1;
            }
            return 0;
        }

    }

    /**
     * Command to delete a given ledger.
     */
    class DeleteLedgerCmd extends MyCommand {
        Options lOpts = new Options();

        DeleteLedgerCmd() {
            super(CMD_DELETELEDGER);
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
            lOpts.addOption("f", "force", false, "Whether to force delete the Ledger without prompt..?");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final long lid = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            if (lid == -1) {
                LOG.error("Must specify a ledger id");
                return -1;
            }

            boolean force = cmdLine.hasOption("f");
            boolean confirm = false;
            if (!force) {
                confirm = IOUtils.confirmPrompt(
                        "Are you sure to delete Ledger : " + ledgerIdFormatter.formatLedgerId(lid) + "?");
            }

            if (force || confirm) {
                ClientConfiguration conf = new ClientConfiguration();
                conf.addConfiguration(bkConf);
                BookKeeper bk = new BookKeeper(conf);
                bk.deleteLedger(lid);
                bk.close();
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "Delete a ledger";
        }

        @Override
        String getUsage() {
            return "deleteledger -ledgerid <ledgerid> [-force]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command to retrieve bookie information like free disk space, etc from all
     * the bookies in the cluster.
     */
    class BookieInfoCmd extends MyCommand {
        Options lOpts = new Options();

        BookieInfoCmd() {
            super(CMD_BOOKIEINFO);
        }

        @Override
        String getDescription() {
            return "Retrieve bookie info such as free and total disk space";
        }

        @Override
        String getUsage() {
            return "bookieinfo";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        String getReadable(long val) {
            String unit[] = {"", "KB", "MB", "GB", "TB" };
            int cnt = 0;
            double d = val;
            while (d > 1000 && cnt < unit.length-1) {
                d = d/1000;
                cnt++;
            }
            DecimalFormat df = new DecimalFormat("#.###");
            df.setRoundingMode(RoundingMode.DOWN);
            return cnt > 0 ? "(" + df.format(d) + unit[cnt] + ")" : unit[cnt];
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ClientConfiguration clientConf = new ClientConfiguration(bkConf);
            clientConf.setDiskWeightBasedPlacementEnabled(true);
            BookKeeper bk = new BookKeeper(clientConf);

            Map<BookieSocketAddress, BookieInfo> map = bk.getBookieInfo();
            if (map.size() == 0) {
                LOG.info("Failed to retrieve bookie information from any of the bookies");
                bk.close();
                return 0;
            }

            LOG.info("Free disk space info:");
            long totalFree = 0, total=0;
            for (Map.Entry<BookieSocketAddress, BookieInfo> e : map.entrySet()) {
                BookieInfo bInfo = e.getValue();
                BookieSocketAddress bookieId = e.getKey();
                LOG.info(getBookieSocketAddrStringRepresentation(bookieId) + ":\tFree: "
                        + bInfo.getFreeDiskSpace() + getReadable(bInfo.getFreeDiskSpace()) + "\tTotal: "
                        + bInfo.getTotalDiskSpace() + getReadable(bInfo.getTotalDiskSpace()));
                totalFree += bInfo.getFreeDiskSpace();
                total += bInfo.getTotalDiskSpace();
            }
            LOG.info("Total free disk space in the cluster:\t" + totalFree + getReadable(totalFree));
            LOG.info("Total disk capacity in the cluster:\t" + total + getReadable(total));
            bk.close();
            return 0;
        }
    }

    /**
     * Command to trigger AuditTask by resetting lostBookieRecoveryDelay to its current value
     */
    class TriggerAuditCmd extends MyCommand {
        Options opts = new Options();

        TriggerAuditCmd() {
            super(CMD_TRIGGERAUDIT);
        }

        @Override
        String getDescription() {
            return "Force trigger the Audit by resetting the lostBookieRecoveryDelay";
        }

        @Override
        String getUsage() {
            return CMD_TRIGGERAUDIT;
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                admin.triggerAudit();
            } finally {
                if (admin != null) {
                    admin.close();
                }
            }
            return 0;
        }
    }
    
    /**
     * Command to trigger AuditTask by resetting lostBookieRecoveryDelay and then make sure the 
     * ledgers stored in the bookie are properly replicated.
     */
    class DecommissionBookieCmd extends MyCommand {
        Options lOpts = new Options();
        
        DecommissionBookieCmd() {
            super(CMD_DECOMMISSIONBOOKIE);
        }

        @Override
        String getDescription() {
            return "Force trigger the Audittask and make sure all the ledgers stored in the decommissioning bookie are replicated";
        }

        @Override
        String getUsage() {
            return CMD_DECOMMISSIONBOOKIE;
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                BookieSocketAddress thisBookieAddress = Bookie.getBookieAddress(bkConf);
                admin.decommissionBookie(thisBookieAddress);
                return 0;
            } catch (Exception e) {
                LOG.error("Received exception in DecommissionBookieCmd ", e);
                return -1;
            } finally {
                if (admin != null) {
                    admin.close();
                }
            }
        }
    }
    
    /**
     * A facility for reporting update ledger progress.
     */
    public interface UpdateLedgerNotifier {
        void progress(long updated, long issued);
    }

    final Map<String, MyCommand> commands = new HashMap<String, MyCommand>();
    {
        commands.put(CMD_METAFORMAT, new MetaFormatCmd());
        commands.put(CMD_INITNEWCLUSTER, new InitNewCluster());
        commands.put(CMD_NUKEEXISTINGCLUSTER, new NukeExistingCluster());
        commands.put(CMD_INITBOOKIE, new InitBookieCmd());
        commands.put(CMD_BOOKIEFORMAT, new BookieFormatCmd());
        commands.put(CMD_RECOVER, new RecoverCmd());
        commands.put(CMD_LEDGER, new LedgerCmd());
        commands.put(CMD_READ_LEDGER_ENTRIES, new ReadLedgerEntriesCmd());
        commands.put(CMD_LISTLEDGERS, new ListLedgersCmd());
        commands.put(CMD_LISTUNDERREPLICATED, new ListUnderreplicatedCmd());
        commands.put(CMD_WHOISAUDITOR, new WhoIsAuditorCmd());
        commands.put(CMD_WHATISINSTANCEID, new WhatIsInstanceId());
        commands.put(CMD_LEDGERMETADATA, new LedgerMetadataCmd());
        commands.put(CMD_SIMPLETEST, new SimpleTestCmd());
        commands.put(CMD_BOOKIESANITYTEST, new BookieSanityTestCmd());
        commands.put(CMD_READLOG, new ReadLogCmd());
        commands.put(CMD_READLOGMETADATA, new ReadLogMetadataCmd());
        commands.put(CMD_READJOURNAL, new ReadJournalCmd());
        commands.put(CMD_LASTMARK, new LastMarkCmd());
        commands.put(CMD_AUTORECOVERY, new AutoRecoveryCmd());
        commands.put(CMD_LISTBOOKIES, new ListBookiesCmd());
        commands.put(CMD_LISTFILESONDISC, new ListDiskFilesCmd());
        commands.put(CMD_UPDATECOOKIE, new UpdateCookieCmd());
        commands.put(CMD_UPDATELEDGER, new UpdateLedgerCmd());
        commands.put(CMD_DELETELEDGER, new DeleteLedgerCmd());
        commands.put(CMD_BOOKIEINFO, new BookieInfoCmd());
        commands.put(CMD_DECOMMISSIONBOOKIE, new DecommissionBookieCmd());
        commands.put(CMD_HELP, new HelpCmd());
        commands.put(CMD_LOSTBOOKIERECOVERYDELAY, new LostBookieRecoveryDelayCmd());  
        commands.put(CMD_TRIGGERAUDIT, new TriggerAuditCmd());
    }

    @Override
    public void setConf(Configuration conf) throws Exception {
        bkConf.loadConf(conf);
        journalDirectory = Bookie.getCurrentDirectory(bkConf.getJournalDir());
        ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        if (null == bkConf.getIndexDirs()) {
            indexDirectories = ledgerDirectories;
        } else {
            indexDirectories = Bookie.getCurrentDirectories(bkConf.getIndexDirs());
        }

        pageSize = bkConf.getPageSize();
        entriesPerPage = pageSize / 8;
    }

    private void printShellUsage() {
        LOG.error(
                "Usage: bookkeeper shell [-localbookie [<host:port>]] [-ledgeridformat <hex/long/uuid>] [-entryformat <hex/string>] [-conf configuration] <command>");
        LOG.error("where command is one of:");
        List<String> commandNames = new ArrayList<String>();
        for (MyCommand c : commands.values()) {
            commandNames.add("       " + c.getUsage());
        }
        Collections.sort(commandNames);
        for (String s : commandNames) {
            LOG.error(s);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length <= 0) {
            printShellUsage();
            return -1;
        }

        String cmdName = args[0];
        Command cmd = commands.get(cmdName);
        if (null == cmd) {
            LOG.error("ERROR: Unknown command " + cmdName);
            printShellUsage();
            return -1;
        }
        // prepare new args
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        return cmd.runCmd(newArgs);
    }

    /*
     * The string returned is of the form: 'resolved canonicalHostName'/'literal IP address':'port number'
     */
    private static String getBookieSocketAddrStringRepresentation(BookieSocketAddress bookieId) {
        String bookieSocketAddrStringRepresentation = bookieId.getSocketAddress().getAddress().getCanonicalHostName()
                + "/" + bookieId.getSocketAddress().getAddress().getHostAddress() + ":"
                + bookieId.getSocketAddress().getPort();
        return bookieSocketAddrStringRepresentation;
    }
    
    /**
     * Returns the sorted list of the files in the given folders with the given file extensions.
     * Sorting is done on the basis of CreationTime if the CreationTime is not available or if they are equal
     * then sorting is done by LastModifiedTime
     * @param folderNames - array of folders which we need to look recursively for files with given extensions
     * @param extensions - the file extensions, which we are interested in
     * @return sorted list of files
     */
    private static List<File> listFilesAndSort(File[] folderNames, String... extensions) {
        List<File> completeFilesList = new ArrayList<File>();
        for (int i = 0; i < folderNames.length; i++) {
            Collection<File> filesCollection = FileUtils.listFiles(folderNames[i], extensions, true);
            completeFilesList.addAll(filesCollection);
        }
        Collections.sort(completeFilesList, new FilesTimeComparator());
        return completeFilesList;
    }

    private static class FilesTimeComparator implements Comparator<File> {
        @Override
        public int compare(File file1, File file2) {
            Path file1Path = Paths.get(file1.getAbsolutePath());
            Path file2Path = Paths.get(file2.getAbsolutePath());
            try {
                BasicFileAttributes file1Attributes = Files.readAttributes(file1Path, BasicFileAttributes.class);
                BasicFileAttributes file2Attributes = Files.readAttributes(file2Path, BasicFileAttributes.class);
                FileTime file1CreationTime = file1Attributes.creationTime();
                FileTime file2CreationTime = file2Attributes.creationTime();
                int compareValue = file1CreationTime.compareTo(file2CreationTime);
                /*
                 * please check https://docs.oracle.com/javase/7/docs/api/java/nio/file/attribute/BasicFileAttributes.html#creationTime()
                 * So not all file system implementation store creation time, in that case creationTime()
                 * method may return FileTime representing the epoch (1970-01-01T00:00:00Z). So in that case
                 * it would be better to compare lastModifiedTime
                 */
                if (compareValue == 0) {
                    FileTime file1LastModifiedTime = file1Attributes.lastModifiedTime();
                    FileTime file2LastModifiedTime = file2Attributes.lastModifiedTime();
                    compareValue = file1LastModifiedTime.compareTo(file2LastModifiedTime);
                }
                return compareValue;
            } catch (IOException e) {
                return 0;
            }
        }
    }

    public static void main(String argv[]) throws Exception {
        BookieShell shell = new BookieShell();

        // handle some common options for multiple cmds
        Options opts = new Options();
        opts.addOption(CONF_OPT, true, "configuration file");
        opts.addOption(LEDGERID_FORMATTER_OPT, true, "format of ledgerId");
        opts.addOption(ENTRY_FORMATTER_OPT, true, "format of entries");
        BasicParser parser = new BasicParser();
        CommandLine cmdLine = parser.parse(opts, argv, true);

        // load configuration
        CompositeConfiguration conf = new CompositeConfiguration();
        if (cmdLine.hasOption(CONF_OPT)) {
            String val = cmdLine.getOptionValue(CONF_OPT);
            conf.addConfiguration(new PropertiesConfiguration(
                                  new File(val).toURI().toURL()));
        }
        shell.setConf(conf);

        // ledgerid format
        if (cmdLine.hasOption(LEDGERID_FORMATTER_OPT)) {
            String val = cmdLine.getOptionValue(LEDGERID_FORMATTER_OPT);
            shell.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(val, shell.bkConf);
        }
        else {
            shell.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(shell.bkConf);
        }
        LOG.debug("Using ledgerIdFormatter {}", shell.ledgerIdFormatter.getClass());

        // entry format
        if (cmdLine.hasOption(ENTRY_FORMATTER_OPT)) {
            String val = cmdLine.getOptionValue(ENTRY_FORMATTER_OPT);
            shell.entryFormatter = EntryFormatter.newEntryFormatter(val, shell.bkConf);
        }
        else {
            shell.entryFormatter = EntryFormatter.newEntryFormatter(shell.bkConf);
        }
        LOG.debug("Using entry formatter {}", shell.entryFormatter.getClass());

        int res = shell.run(cmdLine.getArgs());
        System.exit(res);
    }

    ///
    /// Bookie File Operations
    ///

    /**
     * Get the ledger file of a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     *
     * @return file object.
     */
    private File getLedgerFile(long ledgerId) {
        String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);
        File lf = null;
        for (File d : indexDirectories) {
            lf = new File(d, ledgerName);
            if (lf.exists()) {
                break;
            }
            lf = null;
        }
        return lf;
    }

    /**
     * Get FileInfo for a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     * @return read only file info instance
     */
    ReadOnlyFileInfo getFileInfo(long ledgerId) throws IOException {
        File ledgerFile = getLedgerFile(ledgerId);
        if (null == ledgerFile) {
            throw new FileNotFoundException("No index file found for ledger " + ledgerId + ". It may be not flushed yet.");
        }
        ReadOnlyFileInfo fi = new ReadOnlyFileInfo(ledgerFile, null);
        fi.readHeader();
        return fi;
    }

    private synchronized void initEntryLogger() throws IOException {
        if (null == entryLogger) {
            // provide read only entry logger
            entryLogger = new ReadOnlyEntryLogger(bkConf);
        }
    }

    /**
     * scan over entry log
     *
     * @param logId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     */
    protected void scanEntryLog(long logId, EntryLogScanner scanner) throws IOException {
        initEntryLogger();
        entryLogger.scanEntryLog(logId, scanner);
    }

    private synchronized Journal getJournal() throws IOException {
        if (null == journal) {
            journal = new Journal(bkConf, new LedgerDirsManager(bkConf, bkConf.getLedgerDirs(),
                    new DiskChecker(bkConf.getDiskUsageThreshold(), bkConf.getDiskUsageWarnThreshold())));
        }
        return journal;
    }

    /**
     * Scan journal file
     *
     * @param journalId
     *          Journal File Id
     * @param scanner
     *          Journal File Scanner
     */
    protected void scanJournal(long journalId, JournalScanner scanner) throws IOException {
        getJournal().scanJournal(journalId, 0L, scanner);
    }

    ///
    /// Bookie Shell Commands
    ///

    /**
     * Read ledger meta
     *
     * @param ledgerId
     *          Ledger Id
     */
    protected void readLedgerMeta(long ledgerId) throws Exception {
        LOG.info("===== LEDGER: " + ledgerIdFormatter.formatLedgerId(ledgerId) + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        byte[] masterKey = fi.getMasterKey();
        if (null == masterKey) {
            LOG.info("master key  : NULL");
        } else {
            LOG.info("master key  : " + bytes2Hex(fi.getMasterKey()));
        }
        long size = fi.size();
        if (size % 8 == 0) {
            LOG.info("size        : " + size);
        } else {
            LOG.info("size : " + size + " (not aligned with 8, may be corrupted or under flushing now)");
        }
        LOG.info("entries     : " + (size / 8));
        LOG.info("isFenced    : " + fi.isFenced());
    }

    /**
     * Read ledger index entires
     *
     * @param ledgerId
     *          Ledger Id
     * @throws IOException
     */
    protected void readLedgerIndexEntries(long ledgerId) throws IOException {
        LOG.info("===== LEDGER: " + ledgerIdFormatter.formatLedgerId(ledgerId) + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        long size = fi.size();
        LOG.info("size        : " + size);
        long curSize = 0;
        long curEntry = 0;
        LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage);
        lep.usePage();
        try {
            while (curSize < size) {
                lep.setLedgerAndFirstEntry(ledgerId, curEntry);
                lep.readPage(fi);

                // process a page
                for (int i=0; i<entriesPerPage; i++) {
                    long offset = lep.getOffset(i * 8);
                    if (0 == offset) {
                        LOG.info("entry " + curEntry + "\t:\tN/A");
                    } else {
                        long entryLogId = offset >> 32L;
                        long pos = offset & 0xffffffffL;
                        LOG.info("entry " + curEntry + "\t:\t(log:" + entryLogId + ", pos: " + pos + ")");
                    }
                    ++curEntry;
                }

                curSize += pageSize;
            }
        } catch (IOException ie) {
            LOG.error("Failed to read index page : ", ie);
            if (curSize + pageSize < size) {
                LOG.info("Failed to read index page @ " + curSize + ", the index file may be corrupted : " + ie.getMessage());
            } else {
                LOG.info("Failed to read last index page @ " + curSize
                                 + ", the index file may be corrupted or last index page is not fully flushed yet : " + ie.getMessage());
            }
        }
    }

    protected void printEntryLogMetadata(long logId) throws IOException {
        LOG.info("Print entryLogMetadata of entrylog " + logId + " (" + Long.toHexString(logId) + ".log)");
        initEntryLogger();
        EntryLogMetadata entryLogMetadata = entryLogger.getEntryLogMetadata(logId);
        Map<Long, Long> entryLogMetadataLedgersMap = entryLogMetadata.getLedgersMap();

        for (Entry<Long, Long> sizeOfALedger : entryLogMetadataLedgersMap.entrySet()) {
            LOG.info("--------- Lid=" + ledgerIdFormatter.formatLedgerId(sizeOfALedger.getKey())
                    + ", TotalSizeOfEntriesOfLedger=" + sizeOfALedger.getValue() + " ---------");
        }
    }

    /**
     * Scan over an entry log file.
     *
     * @param logId
     *          Entry Log File id.
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanEntryLog(long logId, final boolean printMsg) throws Exception {
        LOG.info("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        scanEntryLog(logId, new EntryLogScanner() {
            @Override
            public boolean accept(long ledgerId) {
                return true;
            }
            @Override
            public void process(long ledgerId, long startPos, ByteBuffer entry) {
                formatEntry(startPos, entry, printMsg);
            }
        });
    }

    /**
     * Scan over an entry log file for a particular entry
     *
     * @param logId
     *          Entry Log File id.
     * @param ledgerId
     *          id of the ledger
     * @param entryId
     *          entryId of the ledger we are looking for (-1 for all of the entries of the ledger)
     * @param printMsg
     *          Whether printing the entry data.
     * @throws Exception
     */
    protected void scanEntryLogForSpecificEntry(long logId, final long lId, final long eId, final boolean printMsg)
            throws Exception {
        LOG.info("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)" + " for LedgerId " + lId
                + ((eId == -1) ? "" : " for EntryId " + eId));
        final MutableBoolean entryFound = new MutableBoolean(false);
        scanEntryLog(logId, new EntryLogScanner() {
            @Override
            public boolean accept(long ledgerId) {
                return ((lId == ledgerId) && ((!entryFound.booleanValue()) || (eId == -1)));
            }

            @Override
            public void process(long ledgerId, long startPos, ByteBuffer entry) {
                long entrysLedgerId = entry.getLong();
                long entrysEntryId = entry.getLong();
                entry.rewind();
                if ((ledgerId == entrysLedgerId) && (ledgerId == lId) && ((entrysEntryId == eId)) || (eId == -1)) {
                    entryFound.setValue(true);
                    formatEntry(startPos, entry, printMsg);
                }
            }
        });
        if (!entryFound.booleanValue()) {
            LOG.info("LedgerId " + lId + ((eId == -1) ? "" : " EntryId " + eId)
                    + " is not available in the entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        }
    }

    /**
     * Scan over an entry log file for entries in the given position range
     *
     * @param logId
     *          Entry Log File id.
     * @param rangeStartPos
     *          Start position of the entry we are looking for
     * @param rangeEndPos
     *          End position of the entry we are looking for (-1 for till the end of the entrylog)
     * @param printMsg
     *          Whether printing the entry data.
     * @throws Exception
     */
    protected void scanEntryLogForPositionRange(long logId, final long rangeStartPos, final long rangeEndPos, final boolean printMsg)
 throws Exception {
        LOG.info("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)" + " for PositionRange: "
                + rangeStartPos + " - " + rangeEndPos);
        final MutableBoolean entryFound = new MutableBoolean(false);
        scanEntryLog(logId, new EntryLogScanner() {
            private MutableBoolean stopScanning = new MutableBoolean(false);

            @Override
            public boolean accept(long ledgerId) {
                return !stopScanning.booleanValue();
            }

            @Override
            public void process(long ledgerId, long entryStartPos, ByteBuffer entry) {
                if (!stopScanning.booleanValue()) {
                    if ((rangeEndPos != -1) && (entryStartPos > rangeEndPos)) {
                        stopScanning.setValue(true);
                    } else {
                        int entrySize = entry.limit();
                        /**
                         * entrySize of an entry (inclusive of payload and
                         * header) value is stored as int value in log file, but
                         * it is not counted in the entrySize, hence for calculating
                         * the end position of the entry we need to add additional
                         * 4 (intsize of entrySize). Please check
                         * EntryLogger.scanEntryLog.
                         */
                        long entryEndPos = entryStartPos + entrySize + 4 - 1;
                        if (((rangeEndPos == -1) || (entryStartPos <= rangeEndPos)) && (rangeStartPos <= entryEndPos)) {
                            formatEntry(entryStartPos, entry, printMsg);
                            entryFound.setValue(true);
                        }
                    }
                }
            }
        });
        if (!entryFound.booleanValue()) {
            LOG.info("Entry log " + logId + " (" + Long.toHexString(logId)
                    + ".log) doesn't has any entry in the range " + rangeStartPos + " - " + rangeEndPos
                    + ". Probably the position range, you have provided is lesser than the LOGFILE_HEADER_SIZE (1024) "
                    + "or greater than the current log filesize.");
        }
    }

    /**
     * Scan a journal file
     *
     * @param journalId
     *          Journal File Id
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanJournal(long journalId, final boolean printMsg) throws Exception {
        LOG.info("Scan journal " + journalId + " (" + Long.toHexString(journalId) + ".txn)");
        scanJournal(journalId, new JournalScanner() {
            boolean printJournalVersion = false;
            @Override
            public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                if (!printJournalVersion) {
                    LOG.info("Journal Version : " + journalVersion);
                    printJournalVersion = true;
                }
                formatEntry(offset, entry, printMsg);
            }
        });
    }

    /**
     * Print last log mark
     */
    protected void printLastLogMark() throws IOException {
        LogMark lastLogMark = getJournal().getLastLogMark().getCurMark();
        LOG.info("LastLogMark: Journal Id - " + lastLogMark.getLogFileId() + "("
                + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - "
                + lastLogMark.getLogFileOffset());
    }

    /**
     * Format the entry into a readable format.
     *
     * @param entry
     *          ledgerentry to print
     * @param printMsg
     *          Whether printing the message body
     */
    private void formatEntry(LedgerEntry entry, boolean printMsg) {
        long ledgerId = entry.getLedgerId();
        long entryId = entry.getEntryId();
        long entrySize = entry.getLength();
        LOG.info("--------- Lid=" + ledgerIdFormatter.formatLedgerId(ledgerId) + ", Eid=" + entryId + ", EntrySize=" + entrySize + " ---------");
        if (printMsg) {
            entryFormatter.formatEntry(entry.getEntry());
        }
    }

    /**
     * Format the message into a readable format.
     *
     * @param pos
     *          File offset of the message stored in entry log file
     * @param recBuff
     *          Entry Data
     * @param printMsg
     *          Whether printing the message body
     */
    private void formatEntry(long pos, ByteBuffer recBuff, boolean printMsg) {
        long ledgerId = recBuff.getLong();
        long entryId = recBuff.getLong();
        int entrySize = recBuff.limit();

        LOG.info("--------- Lid=" + ledgerIdFormatter.formatLedgerId(ledgerId) + ", Eid=" + entryId
                         + ", ByteOffset=" + pos + ", EntrySize=" + entrySize + " ---------");
        if (entryId == Bookie.METAENTRY_ID_LEDGER_KEY) {
            int masterKeyLen = recBuff.getInt();
            byte[] masterKey = new byte[masterKeyLen];
            recBuff.get(masterKey);
            LOG.info("Type:           META");
            LOG.info("MasterKey:      " + bytes2Hex(masterKey));
            LOG.info("");
            return;
        }
        if (entryId == Bookie.METAENTRY_ID_FENCE_KEY) {
            LOG.info("Type:           META");
            LOG.info("Fenced");
            LOG.info("");
            return;
        }
        // process a data entry
        long lastAddConfirmed = recBuff.getLong();
        LOG.info("Type:           DATA");
        LOG.info("LastConfirmed:  " + lastAddConfirmed);
        if (!printMsg) {
            LOG.info("");
            return;
        }
        // skip digest checking
        recBuff.position(32 + 8);
        LOG.info("Data:");
        LOG.info("");
        try {
            byte[] ret = new byte[recBuff.remaining()];
            recBuff.get(ret);
            entryFormatter.formatEntry(ret);
        } catch (Exception e) {
            LOG.error("N/A. Corrupted.");
        }
        LOG.info("");
    }

    static String bytes2Hex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        Formatter formatter = new Formatter(sb);
        for (byte b : data) {
            formatter.format("%02x", b);
        }
        formatter.close();
        return sb.toString();
    }

    private static int getOptionIntValue(CommandLine cmdLine, String option, int defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException nfe) {
                LOG.error("ERROR: invalid value for option " + option + " : " + val);
                return defaultVal;
            }
        }
        return defaultVal;
    }

    private static long getOptionLongValue(CommandLine cmdLine, String option, long defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException nfe) {
                LOG.error("ERROR: invalid value for option " + option + " : " + val);
                return defaultVal;
            }
        }
        return defaultVal;
    }

    private long getOptionLedgerIdValue(CommandLine cmdLine, String option, long defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return ledgerIdFormatter.readLedgerId(val);
            } catch (IllegalArgumentException iae) {
                LOG.error("ERROR: invalid value for option " + option + " : " + val);
                return defaultVal;
            }
        }
        return defaultVal;
    }

    private static boolean getOptionBooleanValue(CommandLine cmdLine, String option, boolean defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            return Boolean.parseBoolean(val);
        }
        return defaultVal;
    }

    private static boolean getOptionalValue(String optValue, String optName) {
        if (StringUtils.equals(optValue, optName)) {
            return true;
        }
        return false;
    }
}
