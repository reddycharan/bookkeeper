package org.apache.bookkeeper.client;
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

import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger entry. Its a simple tuple containing the ledger id, the entry-id, and
 * the entry content.
 *
 */

public class LedgerEntry {
    private final static Logger LOG = LoggerFactory.getLogger(LedgerEntry.class);

    // provider used to get new byte array
    // i.e. to get it from pool or create a new one
    @FunctionalInterface
    public interface ByteArrayProvider {
        public byte[] getArray(int size) throws Exception;
    }

    // compliments ByteArrayProvider
    // handles discards of the data
    @FunctionalInterface
    public interface ByteArrayDiscarder {
        public void discard(byte[] arr);
    }
    
    long ledgerId;
    long entryId;
    long length;
    ByteBufInputStream entryDataStream;

    LedgerEntry(long lId, long eId) {
        this.ledgerId = lId;
        this.entryId = eId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public long getLength() {
        return length;
    }

    // can provide allocator (i.e. pool) to reduce generated garbage
    // provider used to get new byte array
    // discarder only used if error happened, to properly discard array from provider
    // to not break previous behavior, on error returns ByteBuffer.wrap(new byte[0]) 
    public ByteBuffer getEntry(ByteArrayProvider provider, ByteArrayDiscarder discarder) {
        byte[] ret = null;
        
        // In general, you can't rely on the available() method of an input
        // stream, but ChannelBufferInputStream is backed by a byte[] so it
        // accurately knows the # bytes available
        final int sz;
        try {
        	sz = entryDataStream.available();
        } catch (IOException e) {
            // The channelbufferinput stream doesn't really throw the
            // ioexceptions, it just has to be in the signature because
            // InputStream says so. Hence this code, should never be reached.
            LOG.error("Unexpected Exception while reading from channel buffer", e);
            return ByteBuffer.wrap(new byte[0]);
        }
        
        try {
			ret = provider.getArray(sz);
		} catch (Exception e) {
			LOG.error("ByteArrayProvider failed to provide array", e);
		}
        
		if (ret != null && ret.length < sz) {
			LOG.error("ByteArrayProvider failed to provide array of required size {}, got", 
					sz, ret.length);
			discarder.discard(ret);
			ret = null;
		}
		
        if (ret == null) {
			LOG.error("ByteArrayProvider failed to provide array, got null");
			return ByteBuffer.wrap(new byte[0]);
        }
        
        try {
			entryDataStream.readFully(ret, 0, sz);
	        final ByteBuffer bb = ByteBuffer.wrap(ret, 0, sz);
	        bb.position(sz);
	        return bb;
		} catch (IOException e) {
			discarder.discard(ret);
            // The channelbufferinput stream doesn't really throw the
            // ioexceptions, it just has to be in the signature because
            // InputStream says so. Hence this code, should never be reached.
            LOG.error("Unexpected Exception while reading from channel buffer", e);
            return ByteBuffer.wrap(new byte[0]);
		}
    }

    // old style: create new array each time, let GC pick it up
    public byte[] getEntry() {
        return getEntry(x -> new byte[x], y -> {}).array();
   }

    public InputStream getEntryInputStream() {
        return entryDataStream;
    }
}
