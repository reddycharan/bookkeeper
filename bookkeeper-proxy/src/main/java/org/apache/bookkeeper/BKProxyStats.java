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

package org.apache.bookkeeper;

public interface BKProxyStats {

	//Scopes
	public final static String PROXY_WORKER_SCOPE = "proxy_worker";

    //BKProxyMain
    public final static String LEDGER_CREATION_TIME = "LEDGER_CREATION_TIME";
    public final static String LEDGER_RECOVERY_READ_TIME = "LEDGER_RECOVERY_READ_TIME";
    public final static String LEDGER_NON_RECOVERY_READ_TIME = "LEDGER_NON_RECOVERY_READ_TIME";
    public final static String LEDGER_STAT_TIME = "LEDGER_STAT_TIME";
    public final static String LEDGER_DELETE_ALL_TIME = "LEDGER_DELETE_ALL_TIME";
    public final static String LEDGER_WRITE_CLOSE_TIME = "LEDGER_WRITE_CLOSE_TIME";
    public final static String LEDGER_READ_CLOSE_TIME = "LEDGER_READ_CLOSE_TIME";
    public final static String LEDGER_DELETE_TIME = "LEDGER_DELETE_TIME";
    public final static String LEDGER_SYNC_PUT_FRAGMENT_TIME = "LEDGER_SYNC_PUT_FRAGMENT_TIME";
    public final static String LEDGER_ASYNC_PUT_FRAGMENT_TIME = "LEDGER_ASYNC_PUT_FRAGMENT_TIME";
    public final static String LEDGER_GET_FRAGMENT_TIME = "LEDGER_GET_FRAGMENT_TIME";
    public final static String WORKER_POOL_COUNT = "WORKER_POOL_COUNT";
    public final static String GET_FRAGMENT_BYTES = "GET_FRAGMENT_BYTES";
    public final static String PUT_FRAGMENT_BYTES = "PUT_FRAGMENT_BYTES";
    public final static String LEDGER_LIST_GET_TIME = "LEDGER_LIST_GET_TIME";
    public final static String LEDGER_ASYNC_PUT_FRAGMENT_STATUS_TIME = "LEDGER_ASYNC_PUT_FRAGMENT_STATUS_TIME";

}
