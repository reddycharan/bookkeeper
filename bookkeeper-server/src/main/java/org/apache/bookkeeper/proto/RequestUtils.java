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
package org.apache.bookkeeper.proto;

import com.google.common.base.MoreObjects;

/**
 * Utilities for requests.
 */
class RequestUtils {

    /**
     * this toSafeString method filters out body and masterKey from the output.
     * masterKey contains the password of the ledger and body is customer data,
     * so it is not appropriate to have these in logs or system output.
     */
    public static String toSafeString(BookkeeperProtocol.Request request) {
        MoreObjects.ToStringHelper stringHelper = MoreObjects.toStringHelper(request);
        BookkeeperProtocol.BKPacketHeader header = request.getHeader();
        if (request.hasAddRequest()) {
            BookkeeperProtocol.AddRequest addRequest = request.getAddRequest();
            includeHeaderFields(stringHelper, header);
            stringHelper.add("ledgerId", addRequest.getLedgerId());
            stringHelper.add("entryId", addRequest.getEntryId());
            if (addRequest.hasFlag()) {
                stringHelper.add("flag", addRequest.getFlag());
            }
            return stringHelper.toString();
        } else if (request.hasReadRequest()) {
            BookkeeperProtocol.ReadRequest readRequest = request.getReadRequest();
            includeHeaderFields(stringHelper, header);
            stringHelper.add("ledgerId", readRequest.getLedgerId());
            stringHelper.add("entryId", readRequest.getEntryId());
            if (readRequest.hasFlag()) {
                stringHelper.add("flag", readRequest.getFlag());
            }
            return stringHelper.toString();
        } else if (request.hasWriteLacRequest()) {
            BookkeeperProtocol.WriteLacRequest writeLacRequest = request.getWriteLacRequest();
            includeHeaderFields(stringHelper, header);
            stringHelper.add("ledgerId", writeLacRequest.getLedgerId());
            stringHelper.add("lac", writeLacRequest.getLac());
            return stringHelper.toString();
        } else {
            return request.toString();
        }
    }

    private static void includeHeaderFields(MoreObjects.ToStringHelper stringHelper,
            BookkeeperProtocol.BKPacketHeader header) {
        stringHelper.add("version", header.getVersion());
        stringHelper.add("operation", header.getOperation());
        stringHelper.add("txnId", header.getTxnId());
    }
}
