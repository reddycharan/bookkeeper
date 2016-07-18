package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.HashSet;

import org.apache.bookkeeper.TestScenarioState.ByteArrayWrapper;

public class LedgerListGetReqBKPOperation extends BKPOperationExtension {

    public static final String extentSplitRegex = ":";
    private HashSet<ByteArrayWrapper> expectedExtentsSet;
    private int expectedNoOfExtents;

    public LedgerListGetReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType,
         byte[] extentId, byte responseType, byte expectedReturnStatus, HashSet<ByteArrayWrapper> expectedExtentsSet) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.expectedExtentsSet = expectedExtentsSet;
        expectedNoOfExtents = expectedExtentsSet.size();
    }

    public HashSet<ByteArrayWrapper> getExpectedExtentsSet() {
        return expectedExtentsSet;
    }

    public int getExpectedNoOfExtents() {
        return expectedNoOfExtents;
    }

    public static LedgerListGetReqBKPOperation createLedgerListGetReqBKPOperation(short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerListGetReq) {
            throw new IllegalArgumentException("Expected LedgerListGetReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);
        String[] expectedExtents = operationParameters[5].split(extentSplitRegex);
        HashSet<ByteArrayWrapper> expectedExtentsSet = new HashSet<ByteArrayWrapper>();
        for (String expectedExtent : expectedExtents) {
            expectedExtentsSet.add(new ByteArrayWrapper(
                    TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(expectedExtent)));
        }

        LedgerListGetReqBKPOperation llgOperation = new LedgerListGetReqBKPOperation(protocolVersion, timeSlot, threadId, requestType,
                extentId, BKPConstants.LedgerListGetResp, expectedReturnStatus, expectedExtentsSet);
        return llgOperation;
    }

    @Override
    public void receivePayloadAndVerify(SocketChannel clientSocketChannel) throws OperationException, IOException {
        HashSet<ByteArrayWrapper> actualExtentsSet = new HashSet<ByteArrayWrapper>();
        int expectedThisBatch = 0, numRemaining = expectedNoOfExtents;

        if (getExpectedReturnStatus() != BKPConstants.SF_OK) {
            return;
        }

        while (numRemaining > 0) {
            if (numRemaining < BKPConstants.LEDGER_LIST_BATCH_SIZE) {
                expectedThisBatch = numRemaining;
            } else {
                expectedThisBatch = BKPConstants.LEDGER_LIST_BATCH_SIZE;
            }
            getIntFromResponseAndVerify(clientSocketChannel, expectedThisBatch, "ExpectedNoOfExtents");
            for (int i = 0; i < expectedThisBatch; i++) {
                ByteBuffer nextExtent = ByteBuffer.allocate(BKPConstants.EXTENTID_SIZE);
                nextExtent.order(ByteOrder.nativeOrder());
                while (nextExtent.hasRemaining()) {
                    clientSocketChannel.read(nextExtent);
                }
                nextExtent.flip();
                byte[] extentArray = nextExtent.array();
                actualExtentsSet.add(new ByteArrayWrapper(extentArray));
            }
            numRemaining -= expectedThisBatch;

            // Receive the next response packet
            receiveResponseAndVerify(clientSocketChannel);
        }

        // To indicate the end of transmission the server sends a packet
        // with 0 entries read that as well
        getIntFromResponseAndVerify(clientSocketChannel, 0, "ExpectedNoOfExtents");

        if (!expectedExtentsSet.containsAll(actualExtentsSet)) {
            throw new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of non-matching extents. "
                            + "Expected NoOfExtentIds: %d, Actual NoOfExtentIds: %d",
                    getTimeSlot(), getThreadId(), expectedNoOfExtents, actualExtentsSet.size()));
        }
    }
}
