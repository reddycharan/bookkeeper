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
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.proto.BookkeeperProtocol.GetListOfEntriesOfALedgerResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

public class GetListOfEntriesOfALedgerProcessorV3 extends PacketProcessorBaseV3 implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(GetListOfEntriesOfALedgerProcessorV3.class);

    public GetListOfEntriesOfALedgerProcessorV3(Request request, Channel channel,
            BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    @Override
    public void safeRun() {
        GetListOfEntriesOfALedgerResponse listOfEntriesOfALedgerResponse = getListOfEntriesOfALedgerResponse();
        if (null != listOfEntriesOfALedgerResponse) {
            Response.Builder response = Response.newBuilder().setHeader(getHeader())
                    .setStatus(listOfEntriesOfALedgerResponse.getStatus())
                    .setGetListOfEntriesOfALedgerResponse(listOfEntriesOfALedgerResponse);
            Response resp = response.build();
            sendResponse(listOfEntriesOfALedgerResponse.getStatus(), resp,
                    requestProcessor.getListOfEntriesOfALedgerRequestStats);
        }
    }
}
