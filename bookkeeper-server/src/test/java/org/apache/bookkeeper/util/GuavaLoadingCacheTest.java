/*
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
 */
package org.apache.bookkeeper.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testing Guava Cache.
 */
public class GuavaLoadingCacheTest {
    public static void main(String[] args) throws InterruptedException {

        AtomicInteger counter = new AtomicInteger(0);

        CacheLoader<Long, String> cacheLoader = new CacheLoader<Long, String>() {
            @Override
            public String load(Long key) throws Exception {
                return new String("SomeString" + counter.getAndIncrement());
            }
        };

        LoadingCache<Long, String> cache = CacheBuilder.newBuilder().build(cacheLoader);
        CountDownLatch latch = new CountDownLatch(1);
        final long key = 100;

        class MyThread implements Runnable {
            final String threadName;

            MyThread(String threadName){
                this.threadName = threadName;
            }

            @Override
            public void run() {
                try {
                    latch.await();
                    String getReturnValue = cache.get(key);
                    System.out.println(threadName + " getReturnValue: " + getReturnValue);
                    /*
                     * what the heck? how can getIfPresent return value can be
                     * null, when previous get call has valueloaded value for
                     * this key.
                     */
                    String getIfPresentReturnValue = cache.getIfPresent(key);
                    System.out.println(threadName + " getIfPresentReturnValue: " + getIfPresentReturnValue);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        Thread thread1 = new Thread(new MyThread("Thread-1"));
        Thread thread2 = new Thread(new MyThread("Thread-2"));
        thread1.start();
        thread2.start();

        latch.countDown();
        thread1.join();
        thread2.join();
    }
}
