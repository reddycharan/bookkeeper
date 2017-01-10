package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.BKProxyWorker.OpStatEntry;
import org.apache.bookkeeper.BKProxyWorker.OpStatEntryTimer;
import org.apache.bookkeeper.client.BKException.Code;

public class LedgerAsyncWriteStatus {
    private volatile boolean inProgress = true;
    private volatile int bkError = Code.UnexpectedConditionException;
    private long qwcLatency = 0; // Quorum Write Completion
    private volatile long actualEntryId; // EntryId number we received from BK client
    private final long expectingEntryId; // entryId number we will be expecting
    private final int fragmentId;
    private CountDownLatch latch;
    private Queue<OpStatEntry> asyncWriteStatQueue;
    private final ByteBuffer bdata;
    private final LedgerPrivateData lpd;

    public LedgerAsyncWriteStatus(int fragmentId, long entryId, LedgerPrivateData ledgerPrivateData, ByteBuffer bdata,
            Queue<OpStatEntry> asyncStatQueue) {
        this.fragmentId = fragmentId;
        this.expectingEntryId = entryId;
        this.asyncWriteStatQueue = asyncStatQueue;
        this.lpd = ledgerPrivateData;
        this.bdata = bdata;
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

    public void setComplete(int result, long qwcLatency, long entryId) {
        this.bkError = result;
        this.actualEntryId = entryId;
        this.inProgress = false;

        // release borrowed buffer back to pool.
        if (this.bdata != null) {
            lpd.getBufferPool().returnBuffer(this.bdata);
        }

        // Update stats
        while (!this.asyncWriteStatQueue.isEmpty()) {
            OpStatEntry osl = this.asyncWriteStatQueue.remove();
            if (this.bkError != Code.OK) {
                osl.markFailure();
            } else {
                osl.markSuccess();
            }
        }
        this.qwcLatency = qwcLatency;
        latch.countDown();
    }

    public byte getResult() throws IOException {
        if (inProgress) {
            return BKPConstants.SF_StatusInProgress;
        }
        // Finished execution.
        return BKPConstants.convertBKtoSFerror(bkError);
    }

    public long getCompletionLatency() {
        return this.qwcLatency;
    }
}
