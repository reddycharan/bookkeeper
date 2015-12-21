package org.apache.bookkeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class BKPClientThread extends Thread {

    private String threadId;
    private CyclicBarrier cycBarrier;
    private AtomicReference<Operation> nextOperationRef;
    private SocketChannel clientSocketChannel;
    private int portNo;
    public static int timeoutDurationInSecs = 4;
    private Throwable threadException;
    private TestScenarioState testScenarioState;

    public BKPClientThread(String threadId, CyclicBarrier cycBarrier, int portNo, TestScenarioState testScenarioState)
            throws IOException {
        this.threadId = threadId;
        this.cycBarrier = cycBarrier;
        this.testScenarioState = testScenarioState;
        nextOperationRef = new AtomicReference<Operation>();
        this.portNo = portNo;
        clientSocketChannel = SocketChannel.open();
        clientSocketChannel.connect(new InetSocketAddress("localhost", portNo));
    }

    public String getThreadId() {
        return threadId;
    }

    public CyclicBarrier getCycBarrier() {
        return cycBarrier;
    }

    public Operation getNextOperation() {
        return nextOperationRef.get();
    }

    public void setNextOperation(Operation operation) {
        nextOperationRef.set(operation);
    }

    public SocketChannel getClientSocketChannel() {
        return clientSocketChannel;
    }

    public int getPortNo() {
        return portNo;
    }

    public Throwable getThreadException() {
        return threadException;
    }

    public void setThreadException(Throwable threadException) {
        this.threadException = threadException;
    }

    @Override
    public void run() {
        TestScenarioState currentTestScenarioState = TestScenarioState.getCurrentTestScenarioState();
        while ((!currentTestScenarioState.isScenarioDone()) && (!currentTestScenarioState.isScenarioFailed())
                && (!cycBarrier.isBroken())) {
            try {
                cycBarrier.await((timeoutDurationInSecs * 1000) + testScenarioState.getAdditionalTimeoutWaitTime(),
                        TimeUnit.MILLISECONDS);
                Operation nextoperation = getNextOperation();
                if (nextoperation != null) {
                    nextoperation.perform(clientSocketChannel);
                }
            }
            // If any of the following exceptions are thrown, then the barrier
            // is broken. In that case for the next iteration, while loop will
            // be exited
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                threadException = new OperationException(String.format(
                        "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected Exception: %s",
                        testScenarioState.getCurrentTimeSlot(), getThreadId(), e), e);
            }
        }
        if (currentTestScenarioState.isScenarioDone()) {
            System.out.println(String.format("TestScenario is Complete, so ThreadId: %s is exiting", threadId));
        } else if (cycBarrier.isBroken()) {
            System.out.println(String.format("CyclicBarrier is broken, so ThreadId: %s is exiting", threadId));
        }
        currentTestScenarioState.getCurrentTestScenarioThreadCountDownLatch().countDown();
    }
}
