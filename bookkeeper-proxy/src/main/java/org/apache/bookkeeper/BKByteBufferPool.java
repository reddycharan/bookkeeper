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

import static org.apache.bookkeeper.BKProxyStats.BYTE_BUF_POOL_ACTIVE;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class BKByteBufferPool {
    private GenericObjectPool<ByteBuffer> byteBufPool;

    // tracks peak value of buffers borrowed at any given time, over the life of the pool
    private final Counter byteBufPoolActive;

    public BKByteBufferPool(BKByteBufferPoolFactory poolFactory, GenericObjectPoolConfig poolConfig,
            StatsLogger statsLogger) {
        this.byteBufPool = new GenericObjectPool<ByteBuffer>(poolFactory, poolConfig);

        // add more buffers to the pool once exhausted
        this.byteBufPool.setMaxTotal(-1);

        this.byteBufPoolActive = statsLogger.getCounter(BYTE_BUF_POOL_ACTIVE);
    }

    public ByteBuffer borrowBuffer() throws NoSuchElementException, Exception {
        ByteBuffer newbuf = byteBufPool.borrowObject();
        byteBufPoolActive.inc();

        return newbuf;
    }

    public void returnBuffer(ByteBuffer byteBuf) {
        byteBufPool.returnObject(byteBuf);
        byteBufPoolActive.dec();
    }

    public void close() {
        byteBufPool.close();
    }
}
