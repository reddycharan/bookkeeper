/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.bookkeeper.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

public class IteratorUtilityTest {

    @Test
    public void testWithPrimitiveItrMerge() {
        long[][] arrays = { 
                { 0, 1, 2 }, 
                { 1, 2 }, 
                { 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 }, 
                { 0, 1, 5 }, 
                { 3 }, 
                { 1, 2, 4, 5, 7, 8 },
                {},
                {},
                { 0 }, 
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            for (int j = i + 1; j < arrays.length; j++) {
                long[] tempArray1 = arrays[i];
                long[] tempArray2 = arrays[j];
                HashSet<Long> unionSet = new HashSet<Long>();
                for (int k = 0; k < tempArray1.length; k++) {
                    unionSet.add(tempArray1[k]);
                }
                for (int k = 0; k < tempArray2.length; k++) {
                    unionSet.add(tempArray2[k]);
                }

                PrimitiveIterator.OfLong primitiveIterator1 = Arrays.stream(tempArray1).iterator();
                PrimitiveIterator.OfLong primitiveIterator2 = Arrays.stream(tempArray2).iterator();

                PrimitiveIterator.OfLong mergedItr = IteratorUtility.mergePrimitiveLongIterator(primitiveIterator1,
                        primitiveIterator2);
                ArrayList<Long> mergedArrayList = new ArrayList<Long>();
                Consumer<Long> addMethod = mergedArrayList::add;
                mergedItr.forEachRemaining(addMethod);
                int mergedListSize = mergedArrayList.size();
                Assert.assertEquals("Size of the mergedArrayList", unionSet.size(), mergedArrayList.size());
                Assert.assertTrue("mergedArrayList should contain all elements in unionSet",
                        mergedArrayList.containsAll(unionSet));
                Assert.assertTrue("Merged Iterator should be sorted", IntStream.range(0, mergedListSize - 1)
                        .allMatch(k -> mergedArrayList.get(k) <= mergedArrayList.get(k + 1)));
                Assert.assertTrue("All elements of tempArray1 should be in mergedArrayList",
                        IntStream.range(0, tempArray1.length).allMatch(k -> mergedArrayList.contains(tempArray1[k])));
                Assert.assertTrue("All elements of tempArray2 should be in mergedArrayList",
                        IntStream.range(0, tempArray2.length).allMatch(k -> mergedArrayList.contains(tempArray2[k])));
            }
        }
    }
}
