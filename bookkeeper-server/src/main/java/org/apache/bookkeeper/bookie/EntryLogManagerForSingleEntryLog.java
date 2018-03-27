/*
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

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EntryLogManagerForSingleEntryLog implements EntryLogger.EntryLogManager {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogManagerForSingleEntryLog.class);

    private EntryLogger.BufferedLogChannel activeLogChannel;
    private Lock lockForActiveLogChannel;
    private final Set<EntryLogger.BufferedLogChannel> rotatedLogChannels;
    private final EntryLoggerAllocator entryLoggerAllocator;
    private final LedgerDirsManager ledgerDirsManager;
    private final List<EntryLogger.EntryLogListener> listeners;
    private final EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;

    EntryLogManagerForSingleEntryLog(LedgerDirsManager ledgerDirsManager,
                                     EntryLoggerAllocator entryLoggerAllocator,
                                     List<EntryLogger.EntryLogListener> listeners,
                                     EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus) {
        rotatedLogChannels = ConcurrentHashMap.newKeySet();
        lockForActiveLogChannel = new ReentrantLock();
        this.ledgerDirsManager = ledgerDirsManager;
        this.entryLoggerAllocator = entryLoggerAllocator;
        this.listeners = listeners;
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;
    }

    /**
     * Creates a new log file.
     */
    @Override
    public void createNewLog(Long ledgerId) throws IOException {
        acquireLock(ledgerId);
        try {
            EntryLogger.BufferedLogChannel logChannel = getCurrentLogForLedger(ledgerId);
            // first tried to create a new log channel. add current log channel to ToFlush list only when
            // there is a new log channel. it would prevent that a log channel is referenced by both
            // *logChannel* and *ToFlush* list.
            if (null != logChannel) {

                // flush the internal buffer back to filesystem but not sync disk
                logChannel.flush();

                // Append ledgers map at the end of entry log
                logChannel.appendLedgersMap();

                EntryLogger.BufferedLogChannel newLogChannel = entryLoggerAllocator.createNewLog(
                        selectDirForNextEntryLog());
                setCurrentLogForLedger(ledgerId, newLogChannel);
                LOG.info("Flushing entry logger {} back to filesystem, pending for syncing entry loggers : {}.",
                         logChannel.getLogId(), getCopyOfRotatedLogChannels());
                for (EntryLogger.EntryLogListener listener : listeners) {
                    listener.onRotateEntryLog();
                }
            } else {
                setCurrentLogForLedger(ledgerId, entryLoggerAllocator.createNewLog(
                                               selectDirForNextEntryLog()));
            }
        } finally {
            releaseLock(ledgerId);
        }
    }

    @Override
    public EntryLogger.BufferedLogChannel createNewLogForCompaction() throws IOException {
        return entryLoggerAllocator.createNewLogForCompaction(selectDirForNextEntryLog());
    }

    File selectDirForNextEntryLog() throws IOException {
        File dirForNextEntryLog;
        List<File> list;

        try {
            list = ledgerDirsManager.getWritableLedgerDirs();
        } catch (NoWritableLedgerDirException nwe) {
            if (!ledgerDirsManager.hasWritableLedgerDirs()) {
                list = ledgerDirsManager.getWritableLedgerDirsForNewLog();
            } else {
                LOG.error("All Disks are not full, but getWritableLedgerDirs threw exception ", nwe);
                throw nwe;
            }
        }

        return getDirForNextEntryLog(list);
    }

    /*
     * since entryLogPerLedger is not enabled, it is just one lock for all
     * ledgers.
     */
    @Override
    public void acquireLock(Long ledgerId) {
        lockForActiveLogChannel.lock();
    }

    @Override
    public void acquireLockByCreatingIfRequired(Long ledgerId) {
        acquireLock(ledgerId);
    }

    @Override
    public void releaseLock(Long ledgerId) {
        lockForActiveLogChannel.unlock();
    }

    @Override
    public void setCurrentLogForLedger(Long ledgerId, EntryLogger.BufferedLogChannel logChannel) {
        acquireLock(ledgerId);
        try {
            EntryLogger.BufferedLogChannel hasToRotateLogChannel = activeLogChannel;
            activeLogChannel = logChannel;
            if (hasToRotateLogChannel != null) {
                rotatedLogChannels.add(hasToRotateLogChannel);
            }
        } finally {
            releaseLock(ledgerId);
        }
    }

    @Override
    public EntryLogger.BufferedLogChannel getCurrentLogForLedger(Long ledgerId) {
        return activeLogChannel;
    }

    @Override
    public Set<EntryLogger.BufferedLogChannel> getCopyOfRotatedLogChannels() {
        return new HashSet<EntryLogger.BufferedLogChannel>(rotatedLogChannels);
    }

    @Override
    public Set<EntryLogger.BufferedLogChannel> getCopyOfCurrentLogs() {
        HashSet<EntryLogger.BufferedLogChannel> copyOfCurrentLogs = new HashSet<EntryLogger.BufferedLogChannel>();
        copyOfCurrentLogs.add(activeLogChannel);
        return copyOfCurrentLogs;
    }

    @Override
    public EntryLogger.BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
        EntryLogger.BufferedLogChannel activeLogChannelTemp = activeLogChannel;
        if ((activeLogChannelTemp != null) && (activeLogChannelTemp.getLogId() == entryLogId)) {
            return activeLogChannelTemp;
        }
        return null;
    }

    @Override
    public void removeFromRotatedLogChannels(EntryLogger.BufferedLogChannel rotatedLogChannelToRemove) {
        rotatedLogChannels.remove(rotatedLogChannelToRemove);
    }

    @Override
    public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
        Collections.shuffle(writableLedgerDirs);
        return writableLedgerDirs.get(0);
    }

    @Override
    public void checkpoint() throws IOException {
        flushRotatedLogs();
    }

    @Override
    public void rollLogs() throws IOException {
        createNewLog(EntryLogger.INVALID_LEDGERID);
    }

    @Override
    public void flushRotatedLogs() throws IOException {
        Set<EntryLogger.BufferedLogChannel> channels = getCopyOfRotatedLogChannels();
        if (null == channels) {
            return;
        }
        for (EntryLogger.BufferedLogChannel channel : channels) {
            channel.flushAndForceWrite(true);
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            EntryLogger.closeFileChannel(channel);
            recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
            removeFromRotatedLogChannels(channel);
            LOG.info("Synced entry logger {} to disk.", channel.getLogId());
        }
    }
}

