package org.apache.bookkeeper.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicWeightedRandomSelectionImpl<T> implements WeightedRandomSelection<T> {
    static final Logger LOG = LoggerFactory.getLogger(DynamicWeightedRandomSelectionImpl.class);

    Double randomMax;
    int maxProbabilityMultiplier;
    Map<T, WeightedObject> weightMap;
    ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    DynamicWeightedRandomSelectionImpl() {
        this(-1);
    }

    DynamicWeightedRandomSelectionImpl(int maxMultiplier) {
        this.maxProbabilityMultiplier = maxMultiplier;
        this.weightMap = new HashMap<T, WeightedObject>();
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public T getNextRandom(Collection<T> selectedNodes) {
        rwLock.readLock().lock();
        try {

        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void setMaxProbabilityMultiplier(int max) {
        this.maxProbabilityMultiplier = max;
    }
}
