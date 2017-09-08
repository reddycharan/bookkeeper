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

package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;

public class LedgerManagerIteratorTest extends LedgerManagerTestCase {
    public LedgerManagerIteratorTest(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls);
    }

    @Test(timeout = 60000)
    public void testIterateNoLedgers() throws Exception {
        LedgerManager lm = getLedgerManager();
        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        if (lri.hasNext())
            lri.next();

        assertEquals(false, lri.hasNext());
        assertEquals(false, lri.hasNext());
    }
    
    @Test(timeout = 60000000)
    public void checkConcurrentModifications() throws Exception {
        LedgerManager lm = getLedgerManager();
        LedgerRangeIterator lri = lm.getLedgerRanges();
        assertNotNull(lri);
        LedgerMetadata meta = new LedgerMetadata(3, 2, 2, BookKeeper.DigestType.CRC32, "passwd".getBytes(), null);

        CountDownLatch latch = new CountDownLatch(1);
        lm.createLedgerMetadata(1234567890123456789L, meta, new CreateCallback(latch));
        latch.await();
        latch = new CountDownLatch(1);
        lm.createLedgerMetadata(1234567891123456789L, meta, new CreateCallback(latch));
        latch.await();

        lri = lm.getLedgerRanges();

        assertEquals(true, lri.hasNext());
        lri.next();
        assertEquals(true, lri.hasNext());
        lri.next();
        assertEquals(false, lri.hasNext());

        ZkUtils.createFullPathOptimistic(zkc, "/ledgers/000", "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // it is ok with the following node (/ledgers/000/0000/0000/0000), but not with (/ledgers/000)
        // ZkUtils.createFullPathOptimistic(zkc, "/ledgers/000/0000/0000/0000", "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        lri = lm.getLedgerRanges();
        assertEquals(true, lri.hasNext());
    }

    class CreateCallback implements GenericCallback {

        CountDownLatch latch;

        public CreateCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void operationComplete(int rc, Object result) {
            if(rc != BKException.Code.OK){
                System.out.println("*********** got nonok rc value " + rc);
                System.exit(-1);
            }
            latch.countDown();
        }
    }
}
