package org.apache.bookkeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

public class BKPClientThread implements Runnable {

    private String threadId;
    private CyclicBarrier cycBarrier;
    private AtomicReference<BKPOperation> nextOperationRef;
    private SocketChannel clientSocketChannel;
    private int portNo;
    public static final int TIMEOUTDURATIONINSECS = 4;

    public BKPClientThread(String threadId, CyclicBarrier cycBarrier, int portNo) throws IOException {
        this.threadId = threadId;
        this.cycBarrier = cycBarrier;
        nextOperationRef = new AtomicReference<BKPOperation>();
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

    public BKPOperation getNextOperation() {
        return nextOperationRef.get();
    }

    public void setNextOperation(BKPOperation bkpOperation) {
        nextOperationRef.set(bkpOperation);
    }

    public SocketChannel getClientSocketChannel() {
        return clientSocketChannel;
    }

    public int getPortNo() {
        return portNo;
    }

    @Override
    public void run() {
        TestScenarioState currentTestScenarioState = TestScenarioState.getCurrentTestScenarioState();
        while ((!currentTestScenarioState.isScenarioDone()) && (!currentTestScenarioState.isScenarioFailed())
                && (!cycBarrier.isBroken())) {
            try {
                cycBarrier.await(TIMEOUTDURATIONINSECS, TimeUnit.SECONDS);
                BKPOperation nextoperation = getNextOperation();
                if (nextoperation != null) {
                    nextoperation.perform(clientSocketChannel);
                }
            }
            // If any of the following exceptions are thrown, then the barrier
            // is broken. In that case for the next iteration, while loop will
            // be exited
            catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
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
