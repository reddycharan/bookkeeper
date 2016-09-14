package org.apache.bookkeeper;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.BKProxyWorker.OpStatEntry;
import org.apache.bookkeeper.BKProxyWorker.OpStatEntryTimer;
import org.apache.bookkeeper.client.BKException.Code;

public class LedgerAsyncWriteStatus {
    private volatile boolean inProgress = true;
    private volatile int bkError = Code.UnexpectedConditionException;
    private long completionTime = 0;
    private volatile long actualEntryId; // EntryId number we received from BK client
    private final long expectingEntryId; // entryId number we will be expecting
    private final int fragmentId;
    private CountDownLatch latch;
    private Queue<OpStatEntry> asyncWriteStatQueue;

    public LedgerAsyncWriteStatus(int fragmentId, long entryId, Queue<OpStatEntry> asyncStatQueue) {
        this.fragmentId = fragmentId;
        this.expectingEntryId = entryId;
        this.asyncWriteStatQueue = asyncStatQueue;
        this.latch = new CountDownLatch(1);
    }

    public long getExpectedEntryId() {
        return expectingEntryId;
    }

    public int getFragmentId() {
        return fragmentId;
    }

    public void waitForResult(long timeout) throws InterruptedException {
        if (timeout < 0) {
            latch.await();
        } else {
            latch.await(timeout, TimeUnit.MILLISECONDS);
        }
    }

    public long getActualEntryId() {
        return this.actualEntryId;
    }

    public void setComplete(int result, long entryId) {
        this.bkError = result;
        this.actualEntryId = entryId;
        this.inProgress = false;
        // Update stats
        while (!this.asyncWriteStatQueue.isEmpty()) {
            OpStatEntry osl = this.asyncWriteStatQueue.remove();
            if (this.bkError != Code.OK) {
                osl.markFailure();
            } else {
                osl.markSuccess();
            }
            if (osl instanceof OpStatEntryTimer) {
                this.completionTime = ((OpStatEntryTimer) osl).getElapsedTime();
            }
        }
        latch.countDown();
    }

    public byte getResult() {
        if (inProgress) {
            return BKPConstants.SF_ErrorInProgress;
        }
        // Finished execution.
        return BKPConstants.convertBKtoSFerror(bkError);
    }

    public long getCompletionTime() {
        return this.completionTime;
    }
}
