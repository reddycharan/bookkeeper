/*
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
package org.apache.bookkeeper.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.math.Quantiles;
import com.google.common.math.Quantiles.ScaleAndIndex;

public class DynamicWeightedRandomSelectionImpl<T> implements WeightedRandomSelection<T> {
    static final Logger LOG = LoggerFactory.getLogger(DynamicWeightedRandomSelectionImpl.class);

    int maxProbabilityMultiplier;
    final long minWeight;
    final Map<T, WeightedObject> weightMap;
    final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    Random rand;

    DynamicWeightedRandomSelectionImpl() {
        this(1, 1);
    }

    DynamicWeightedRandomSelectionImpl(int maxMultiplier) {
        this(maxMultiplier, 1L);
    }

    DynamicWeightedRandomSelectionImpl(int maxMultiplier, long minWeight) {
        this.maxProbabilityMultiplier = maxMultiplier;
        this.weightMap = new HashMap<T, WeightedObject>();
        this.minWeight = minWeight;
        rand = new Random(System.currentTimeMillis());
    }

    @Override
    public void updateMap(Map<T, WeightedObject> updatedMap) {
        rwLock.writeLock().lock();
        try {
            weightMap.clear();
            weightMap.putAll(updatedMap);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public T getNextRandom() {
        rwLock.readLock().lock();
        try {
            return getNextRandom(weightMap.keySet());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public T getNextRandom(Collection<T> selectedNodes) {
        rwLock.readLock().lock();
        try {
            Function<? super T, ? extends Long> weightFunc = (node) -> {
                long weight = 0;
                if ((weightMap.containsKey(node))) {
                    weight = weightMap.get(node).getWeight();
                }
                if (weight <= 0) {
                    weight = minWeight;
                }
                return new Long(1L);
            };
            ArrayList<Long> weightList = selectedNodes.stream().map(weightFunc)
                    .collect(Collectors.toCollection(ArrayList::new));
            ScaleAndIndex median = Quantiles.median();
            long medianWeight = (long) median.compute(weightList);
            long maxWeight = maxProbabilityMultiplier * medianWeight;
            long totalWeight = 0;
            T nextRandomNode = null;
            for (T node : selectedNodes) {
                long weight = 0;
                if ((weightMap.containsKey(node))) {
                    weight = weightMap.get(node).getWeight();
                }
                if (weight <= 0) {
                    weight = minWeight;
                } else if (weight >= maxWeight) {
                    weight = maxWeight;
                }
                long randValue = Math.abs(rand.nextLong()) % (totalWeight + weight);
                if (randValue >= totalWeight) {
                    nextRandomNode = node;
                }
                totalWeight += weight;
            }
            return nextRandomNode;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void setMaxProbabilityMultiplier(int max) {
        this.maxProbabilityMultiplier = max;
    }

}
