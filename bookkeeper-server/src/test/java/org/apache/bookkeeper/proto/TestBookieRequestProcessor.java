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
package org.apache.bookkeeper.proto;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest.Flag;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.junit.Test;

import com.google.protobuf.ByteString;

/**
 * Test utility methods from bookie request processor.
 */
public class TestBookieRequestProcessor {

    @Test
    public void testToString() {
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder();
        headerBuilder.setVersion(ProtocolVersion.VERSION_THREE);
        headerBuilder.setOperation(OperationType.ADD_ENTRY);
        headerBuilder.setTxnId(5L);
        BKPacketHeader header = headerBuilder.build();

        AddRequest addRequest = AddRequest.newBuilder().setLedgerId(10).setEntryId(1)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).build();
        Request request = Request.newBuilder().setHeader(header).setAddRequest(addRequest).build();

        WriteEntryProcessorV3 writeEntryProcessorV3 = new WriteEntryProcessorV3(request, null, null);
        String toString = writeEntryProcessorV3.toString();
        assertFalse("writeEntryProcessorV3's toString should have filtered out body", toString.contains("body"));
        assertFalse("writeEntryProcessorV3's toString should have filtered out masterKey",
                toString.contains("masterKey"));
        assertTrue("writeEntryProcessorV3's toString should contain ledgerId", toString.contains("ledgerId"));
        assertTrue("writeEntryProcessorV3's toString should contain entryId", toString.contains("entryId"));
        assertTrue("writeEntryProcessorV3's toString should contain version", toString.contains("version"));
        assertTrue("writeEntryProcessorV3's toString should contain operation", toString.contains("operation"));
        assertTrue("writeEntryProcessorV3's toString should contain txnId", toString.contains("txnId"));
        assertFalse("writeEntryProcessorV3's toString shouldn't contain flag", toString.contains("flag"));

        addRequest = AddRequest.newBuilder().setLedgerId(10).setEntryId(1)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).setFlag(Flag.RECOVERY_ADD)
                .build();
        request = Request.newBuilder().setHeader(header).setAddRequest(addRequest).build();
        writeEntryProcessorV3 = new WriteEntryProcessorV3(request, null, null);
        toString = writeEntryProcessorV3.toString();
        assertFalse("writeEntryProcessorV3's toString should have filtered out body", toString.contains("body"));
        assertFalse("writeEntryProcessorV3's toString should have filtered out masterKey",
                toString.contains("masterKey"));
        assertTrue("writeEntryProcessorV3's toString should contain flag", toString.contains("flag"));

        ReadRequest readRequest = ReadRequest.newBuilder().setLedgerId(10).setEntryId(23)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes())).build();
        request = Request.newBuilder().setHeader(header).setReadRequest(readRequest).build();
        toString = RequestUtils.toSafeString(request);
        assertFalse("ReadRequest's safeString should have filtered out masterKey", toString.contains("masterKey"));
        assertTrue("ReadRequest's safeString should contain ledgerId", toString.contains("ledgerId"));
        assertTrue("ReadRequest's safeString should contain entryId", toString.contains("entryId"));
        assertTrue("ReadRequest's safeString should contain version", toString.contains("version"));
        assertTrue("ReadRequest's safeString should contain operation", toString.contains("operation"));
        assertTrue("ReadRequest's safeString should contain txnId", toString.contains("txnId"));

        WriteLacRequest writeLacRequest = WriteLacRequest.newBuilder().setLedgerId(10).setLac(23)
                .setMasterKey(ByteString.copyFrom("masterKey".getBytes()))
                .setBody(ByteString.copyFrom("entrydata".getBytes())).build();
        request = Request.newBuilder().setHeader(header).setWriteLacRequest(writeLacRequest).build();
        WriteLacProcessorV3 writeLacProcessorV3 = new WriteLacProcessorV3(request, null, null);
        toString = writeLacProcessorV3.toString();
        assertFalse("writeLacProcessorV3's toString should have filtered out body", toString.contains("body"));
        assertFalse("writeLacProcessorV3's toString should have filtered out masterKey",
                toString.contains("masterKey"));
        assertTrue("writeLacProcessorV3's toString should contain ledgerId", toString.contains("ledgerId"));
        assertTrue("writeLacProcessorV3's toString should contain lac", toString.contains("lac"));
        assertTrue("writeLacProcessorV3's toString should contain version", toString.contains("version"));
        assertTrue("writeLacProcessorV3's toString should contain operation", toString.contains("operation"));
        assertTrue("writeLacProcessorV3's toString should contain txnId", toString.contains("txnId"));
    }
}
