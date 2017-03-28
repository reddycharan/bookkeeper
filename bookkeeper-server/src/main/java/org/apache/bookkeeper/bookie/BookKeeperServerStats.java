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

public interface BookKeeperServerStats {

	// Stat scopes
    public final static String SERVER_SCOPE = "bookkeeper_server";
    public final static String BOOKIE_SCOPE = "bookie";
	public final static String JOURNAL_SCOPE = "journal";
	public final static String LEDGER_SCOPE = "ledger";
    public final static String INDEX_SCOPE = "index";

	public final static String SERVER_STATUS = "SERVER_STATUS";

	// Generic scope-specific operations
	public final static String ADD_ENTRY = "ADD_ENTRY";
	public final static String READ_ENTRY = "READ_ENTRY";

	// Server Operations
	public final static String ADD_ENTRY_REQUEST = "ADD_ENTRY_REQUEST";
    public final static String READ_ENTRY_REQUEST = "READ_ENTRY_REQUEST";
    public final static String READ_ENTRY_FENCE_REQUEST = "READ_ENTRY_FENCE_REQUEST";
    public final static String READ_ENTRY_FENCE_WAIT = "READ_ENTRY_FENCE_WAIT";
    public final static String READ_ENTRY_FENCE_READ = "READ_ENTRY_FENCE_READ";
    public final static String WRITE_LAC = "WRITE_LAC";
    public final static String READ_LAC = "READ_LAC";
    public final static String GET_BOOKIE_INFO = "GET_BOOKIE_INFO";
    public final static String SEND_RESPONSE = "SEND_RESPONSE";

    // Bookie Operations
    public final static String ADD_ENTRY_BYTES = "ADD_ENTRY_BYTES";
    public final static String READ_ENTRY_BYTES = "READ_ENTRY_BYTES";
    public final static String RECOVERY_ADD_ENTRY = "RECOVERY_ADD_ENTRY";

    // Journal Stats
    public final static String PREALLOCATION = "PREALLOCATION";
    public final static String FORCE_WRITE_LATENCY = "FORCE_WRITE_LATENCY";
    public final static String FORCE_WRITE_BATCH_ENTRIES = "FORCE_WRITE_BATCH_ENTRIES";
    public final static String FORCE_WRITE_BATCH_BYTES = "FORCE_WRITE_BATCH_BYTES";
    public final static String FLUSH_LATENCY = "FLUSH_LATENCY";
    public final static String CREATION_LATENCY = "CREATION_LATENCY";
    public final static String CB_THREAD_POOL_SIZE = "CB_THREAD_POOL_SIZE";
    public final static String SYNC = "SYNC";
    public final static String QUEUE_LATENCY = "QUEUE_LATENCY";
    public final static String PROCESS_TIME_LATENCY = "PROCESS_TIME_LATENCY";

    // Ledger Storage Stats
    public final static String STORAGE_GET_OFFSET = "STORAGE_GET_OFFSET";
    public final static String STORAGE_GET_ENTRY = "STORAGE_GET_ENTRY";
    public final static String SKIP_LIST_GET_ENTRY = "SKIP_LIST_GET_ENTRY";
    public final static String SKIP_LIST_PUT_ENTRY = "SKIP_LIST_PUT_ENTRY";
    public final static String SKIP_LIST_SNAPSHOT = "SKIP_LIST_SNAPSHOT";

    // Counters
    public final static String QUEUE_SIZE = "QUEUE_SIZE";
    public final static String READ_BYTES = "READ_BYTES";
    public final static String WRITE_BYTES = "WRITE_BYTES";
    public final static String NUM_MINOR_COMP = "NUM_MINOR_COMP";
    public final static String NUM_MAJOR_COMP = "NUM_MAJOR_COMP";
    public final static String FORCE_WRITE_QUEUE_SIZE = "FORCE_WRITE_QUEUE_SIZE";
    public final static String NUM_FORCE_WRITES = "NUM_FORCE_WRITES";
    public final static String NUM_FLUSH_EMPTY_QUEUE = "NUM_FLUSH_EMPTY_QUEUE";
    public final static String NUM_FLUSH_MAX_OUTSTANDING_BYTES = "NUM_FLUSH_MAX_OUTSTANDING_BYTES";
    public final static String NUM_FLUSH_MAX_WAIT = "NUM_FLUSH_MAX_WAIT";
    public final static String SKIP_LIST_FLUSH_BYTES = "SKIP_LIST_FLUSH_BYTES";
    public final static String SKIP_LIST_THROTTLING = "SKIP_LIST_THROTTLING";
    public final static String READ_LAST_ENTRY_NOENTRY_ERROR = "READ_LAST_ENTRY_NOENTRY_ERROR";
    public final static String LEDGER_CACHE_NUM_EVICTED_LEDGERS = "LEDGER_CACHE_NUM_EVICTED_LEDGERS";

    // Gauge
    public final static String NUM_INDEX_PAGES = "NUM_INDEX_PAGES";
    public final static String NUM_OPEN_LEDGERS = "NUM_OPEN_LEDGERS";
    public final static String FORCE_WRITE_GROUPING_COUNT = "FORCE_WRITE_GROUPING_COUNT";
    public final static String NUM_PENDING_READ = "NUM_PENDING_READ";
    public final static String NUM_PENDING_ADD = "NUM_PENDING_ADD";

    // LedgerDirs Stats
    public final static String WRITABLE_DIRS = "WRITEABLE_DIRS";

    // Garbage collection
    public final static String MINOR_COMPACTION_COUNT = "MINOR_COMPACTION_COUNT";
    public final static String MAJOR_COMPACTION_COUNT = "MAJOR_COMPACTION_COUNT";
    public final static String THREAD_RUNTIME = "THREAD_RUNTIME";
    public final static String RECLAIMED_ENTRY_LOG_SPACE_BYTES = "RECLAIMED_ENTRY_LOG_SPACE_BYTES";
    public final static String ACTIVE_LEDGER_COUNT = "ACTIVE_LEDGER_COUNT";
    public final static String RECLAIMED_COMPACTION_SPACE_BYTES = "RECLAIMED_COMPACTION_SPACE_BYTES";
    public final static String DELETED_LEDGER_COUNT = "DELETED_LEDGER_COUNT";
    public final static String ACTIVE_ENTRY_LOG_COUNT = "ACTIVE_ENTRY_LOG_COUNT";
    public final static String ACTIVE_ENTRY_LOG_SPACE_BYTES = "ACTIVE_ENTRY_LOG_SPACE_BYTES";
    public final static String TOTAL_RECLAIMED_SPACE = "TOTAL_RECLAIMED_SPACE";

    //Watcher Operations
    public final static String NEW_ENSEMBLE_TIME = "NEW_ENSEMBLE_TIME";
    public final static String REPLACE_BOOKIE_TIME = "REPLACE_BOOKIE_TIME";

}
