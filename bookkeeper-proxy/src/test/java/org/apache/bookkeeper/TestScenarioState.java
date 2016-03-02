package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;

public class TestScenarioState {

    private static TestScenarioState currentTestScenario;
    public static final int MAXNUMOFFRAGMENTS = 20;

    private HashMap<String, BKProxyMain> bkProxiesMap;
    private HashMap<String, Long> extentIdMap;
    private ConcurrentHashMap<ByteArrayWrapper, AtomicReferenceArray<byte[]>> confirmedFragmentsOfExtents;
    private ConcurrentHashMap<ByteArrayWrapper, AtomicReferenceArray<byte[]>> inFlightFragmentsOfExtents;
    private AtomicBoolean scenarioDone;
    private AtomicBoolean scenarioFailed;
    private Vector<Operation> failedOperations;
    private int numberOfTimeSlots;
    private AtomicInteger currentTimeSlot;
    private ArrayList<Operation>[] operations;
    private HashMap<String, BKPClientThread> thisTestScenarioThreads;
    private TestScenarioCyclicBarrier cycBarrier;
    private CountDownLatch currentTestScenarioThreadCountDownLatch;
    private Random rand;
    private BookKeeperProxyConfiguration commonBKPConfig;
    private long additionalTimeoutWaitTime = 0;

    private TestScenarioState() {
        this.extentIdMap = new HashMap<String, Long>();
        this.confirmedFragmentsOfExtents = new ConcurrentHashMap<ByteArrayWrapper, AtomicReferenceArray<byte[]>>();
        this.inFlightFragmentsOfExtents = new ConcurrentHashMap<ByteArrayWrapper, AtomicReferenceArray<byte[]>>();
        scenarioDone = new AtomicBoolean(false);
        scenarioFailed = new AtomicBoolean(false);
        bkProxiesMap = new HashMap<String, BKProxyMain>();
        thisTestScenarioThreads = new HashMap<String, BKPClientThread>();
        currentTimeSlot = new AtomicInteger(-1);
        failedOperations = new Vector<Operation>();
        rand = new Random();
        commonBKPConfig = new BookKeeperProxyConfiguration();
    }

    public void addAndStartBKP(String bkProxyName, int bkProxyPort) throws InterruptedException {
        BookKeeperProxyConfiguration thisBKPConfig = new BookKeeperProxyConfiguration(commonBKPConfig);
        thisBKPConfig.setBKProxyPort(bkProxyPort);
        BKProxyMain bkProxy = new BKProxyMain(thisBKPConfig);
        Thread bkProxyThread = new Thread(bkProxy);
        bkProxyThread.start();
        Thread.sleep(2000);
        bkProxiesMap.put(bkProxyName, bkProxy);
    }

    public void shutDownAllBkProxies() {
        for (String bkProxyName : bkProxiesMap.keySet()) {
            BKProxyMain bkProxy = bkProxiesMap.get(bkProxyName);
            try {
                bkProxy.shutdown();
            } catch (IOException | InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }
    }

    public static TestScenarioState instantiateCurrentTestScenarioState() {
        currentTestScenario = new TestScenarioState();
        return currentTestScenario;
    }

    public static TestScenarioState getCurrentTestScenarioState() {
        return currentTestScenario;
    }

    public static void clearCurrentTestScenarioState() {
        currentTestScenario = null;
    }

    public void setNumberOfThreads(int noOfThreads) {
        cycBarrier = new TestScenarioCyclicBarrier(noOfThreads, this);
        currentTestScenarioThreadCountDownLatch = new CountDownLatch(noOfThreads);
    }

    public TestScenarioCyclicBarrier getCycBarrier() {
        return cycBarrier;
    }

    public void addBKPClientThread(String threadId, String bkpId) throws IOException {
        int bkpPort = bkProxiesMap.get(bkpId).getBookKeeperProxyConfiguration().getBKProxyPort();
        BKPClientThread bkpThread = new BKPClientThread(threadId, cycBarrier, bkpPort, this);
        thisTestScenarioThreads.put(threadId, bkpThread);
    }

    public BKPClientThread getBKPClientThread(String threadId) {
        return thisTestScenarioThreads.get(threadId);
    }

    public Set<String> getThreadIds() {
        Set<String> threadIdsSet = thisTestScenarioThreads.keySet();
        return threadIdsSet;
    }

    public void closeAllClientSocketChannels() {
        for (String threadId : thisTestScenarioThreads.keySet()) {
            SocketChannel clientSocketChannel = thisTestScenarioThreads.get(threadId).getClientSocketChannel();
            try {
                clientSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public CountDownLatch getCurrentTestScenarioThreadCountDownLatch() {
        return currentTestScenarioThreadCountDownLatch;
    }

    public int getNumberOfTimeSlots() {
        return numberOfTimeSlots;
    }

    @SuppressWarnings("unchecked")
    public void setNumberOfTimeSlots(int n) {
        numberOfTimeSlots = n;
        operations = (ArrayList<Operation>[]) new ArrayList[numberOfTimeSlots];
    }

    public int getCurrentTimeSlot() {
        return currentTimeSlot.get();
    }

    public void setCurrentTimeSlot(int timeSlot) {
        currentTimeSlot.set(timeSlot);
    }

    public void addOperation(int timeSlot, Operation operation) {
        if (operations[timeSlot] == null) {
            operations[timeSlot] = new ArrayList<Operation>();
        }
        operations[timeSlot].add(operation);
    }

    public List<Operation> getOperations(int timeSlot) {
        return operations[timeSlot];
    }

    public void newExtentCreated(byte[] extentId) {
        confirmedFragmentsOfExtents.put(new ByteArrayWrapper(extentId),
                new AtomicReferenceArray<byte[]>(MAXNUMOFFRAGMENTS));
        inFlightFragmentsOfExtents.put(new ByteArrayWrapper(extentId),
                new AtomicReferenceArray<byte[]>(MAXNUMOFFRAGMENTS));
    }

    public void newFragmentAdded(byte[] extentId, int fragmentId, byte[] fragment) {
        AtomicReferenceArray<byte[]> fragmentsArray = confirmedFragmentsOfExtents.get(new ByteArrayWrapper(extentId));
        fragmentsArray.set(fragmentId, fragment);
    }

    public void newFragmentInFlight(byte[] extentId, int fragmentId, byte[] fragment) {
        AtomicReferenceArray<byte[]> fragmentsArray = inFlightFragmentsOfExtents.get(new ByteArrayWrapper(extentId));
        fragmentsArray.set(fragmentId, fragment);
    }

    public byte[] getConfirmedFragment(byte[] extentId, int fragmentId) {
        AtomicReferenceArray<byte[]> fragmentsArray = confirmedFragmentsOfExtents.get(new ByteArrayWrapper(extentId));
        return fragmentsArray.get(fragmentId);
    }

    public byte[] getInFlightFragment(byte[] extentId, int fragmentId) {
        AtomicReferenceArray<byte[]> fragmentsArray = inFlightFragmentsOfExtents.get(new ByteArrayWrapper(extentId));
        return fragmentsArray.get(fragmentId);
    }

    public void removeInFlightFragment(byte[] extentId, int fragmentId) {
        AtomicReferenceArray<byte[]> fragmentsArray = inFlightFragmentsOfExtents.get(new ByteArrayWrapper(extentId));
        fragmentsArray.set(fragmentId, null);
    }

    public int getLastConfirmedFragmentId(byte[] extentId) {
        int lastConfirmedFragmentId = -1;
        AtomicReferenceArray<byte[]> fragmentsArray = confirmedFragmentsOfExtents.get(new ByteArrayWrapper(extentId));
        if (fragmentsArray.get(0) != null) {
            lastConfirmedFragmentId = 0;
        } else if (fragmentsArray.get(1) != null) {
            for (int i = 2; i < fragmentsArray.length(); i++) {
                if (fragmentsArray.get(i) == null) {
                    lastConfirmedFragmentId = i - 1;
                    break;
                }
            }
        }
        return lastConfirmedFragmentId;
    }

    public boolean isScenarioDone() {
        return scenarioDone.get();
    }

    public void setScenarioDone(boolean done) {
        scenarioDone.set(done);
    }

    public boolean isScenarioFailed() {
        return scenarioFailed.get();
    }

    public void setScenarioFailed(boolean scenarioFailed) {
        this.scenarioFailed.set(scenarioFailed);
    }

    public void addFailedOperation(Operation operation) {
        failedOperations.add(operation);
    }

    public Vector<Operation> getFailedOperations() {
        return failedOperations;
    }

    public BookKeeperProxyConfiguration getCommonBKPConfig() {
        return commonBKPConfig;
    }

    public long getAdditionalTimeoutWaitTime() {
        return additionalTimeoutWaitTime;
    }

    public void setAdditionalTimeoutWaitTime(long additionalTimeoutWaitTime) {
        this.additionalTimeoutWaitTime = additionalTimeoutWaitTime;
    }

    public Long getExtentLong(String extentId) {
        Long extentLong;
        if (extentIdMap.containsKey(extentId)) {
            extentLong = extentIdMap.get(extentId);
        } else {
            extentLong = Math.abs(rand.nextLong());
            extentIdMap.put(extentId, extentLong);
        }
        return extentLong;
    }

    public byte[] getExtentIDBytes(String extentId) {
        Long extentLong = getExtentLong(extentId);
        byte[] bytes = new byte[16];
        ByteBuffer b = ByteBuffer.wrap(bytes);
        b.putLong(0);
        b.putLong(extentLong);
        return b.array();
    }

    static final class ByteArrayWrapper {
        private final byte[] data;

        ByteArrayWrapper(byte[] data) {
            if (data == null) {
                throw new NullPointerException();
            }
            this.data = data;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ByteArrayWrapper)) {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}
