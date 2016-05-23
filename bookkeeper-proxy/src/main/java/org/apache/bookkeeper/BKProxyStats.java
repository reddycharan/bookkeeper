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

    //BKProxyMain
    public final static String PXY_LEDGER_CREATION_TIME = "PXY_LEDGER_CREATION_TIME";
    public final static String PXY_LEDGER_RECOVERY_READ_TIME = "PXY_LEDGER_RECOVERY_READ_TIME";
    public final static String PXY_LEDGER_NON_RECOVERY_READ_TIME = "PXY_LEDGER_NON_RECOVERY_READ_TIME";
    public final static String PXY_LEDGER_STAT_TIME = "PXY_LEDGER_STAT_TIME";
    public final static String PXY_LEDGER_DELETE_ALL_TIME = "PXY_LEDGER_DELETE_ALL_TIME";
    public final static String PXY_LEDGER_WRITE_CLOSE_TIME = "PXY_LEDGER_WRITE_CLOSE_TIME";
    public final static String PXY_LEDGER_READ_CLOSE_TIME = "PXY_LEDGER_READ_CLOSE_TIME";
    public final static String PXY_LEDGER_DELETE_TIME = "PXY_LEDGER_DELETE_TIME";
    public final static String PXY_LEDGER_PUT_FRAGMENT_TIME = "PXY_LEDGER_PUT_FRAGMENT_TIME";
    public final static String PXY_LEDGER_GET_FRAGMENT_TIME = "PXY_LEDGER_GET_FRAGMENT_TIME";
    public final static String PXY_WORKER_POOL_COUNT = "PXY_WORKER_POOL_COUNT";
    public final static String PXY_BYTES_GET_FRAGMENT_HIST = "PXY_BYTES_GET_FRAGMENT_HIST";
    public final static String PXY_BYTES_PUT_FRAGMENT_HIST = "PXY_BYTES_PUT_FRAGMENT_HIST";
    public final static String PXY_LEDGER_LIST_GET_TIME = "PXY_LEDGER_LIST_GET_TIME";

}
