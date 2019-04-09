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
package org.apache.bookkeeper.util;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.PrimitiveIterator;

import org.junit.Test;

public class AvailabilityOfEntriesOfLedgerTest {
    @Test
    public void testSimple() {
        long[][] arrays = { 
                { 1, 2 }, 
                { 1, 2, 3, 4, 5, 6, 7, 8 },
                { 1, 2 }
        };
        for (int i = 0; i < arrays.length; i++) {
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(arrays[i]).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            assertEquals("Expected total number of entries", arrays[i].length,
                    availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
        }
    }
}
