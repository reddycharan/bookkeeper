package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;

public class LedgerWriteEntryReqBKPOperation extends BKPOperationExtension {

    private int fragmentID;
    private int size;
    private byte[] fragArray;
    private boolean newRequest;
    private boolean expectingResponse;
    public static final int WAITTIMEBEFORERESPONSECHECKINSECS = 2;

    public LedgerWriteEntryReqBKPOperation(int timeSlot, String threadId, byte requestType, boolean newRequest,
            boolean expectingResponse, byte[] extentId, int fragmentID, int size, byte responseType,
            byte expectedReturnStatus) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.newRequest = newRequest;
        this.expectingResponse = expectingResponse;
        this.fragmentID = fragmentID;
        this.size = size;
    }

    public int getFragmentID() {
        return fragmentID;
    }

    public int getSize() {
        return size;
    }

    public static LedgerWriteEntryReqBKPOperation createLedgerWriteEntryReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerWriteEntryReq) {
            throw new IllegalArgumentException("Expected LedgerWriteEntryReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        boolean newRequest = Boolean.valueOf(operationParameters[3]);
        boolean expectingResponse = Boolean.valueOf(operationParameters[4]);

        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[5]);
        int fragmentID = Integer.valueOf(operationParameters[6]);
        int size = Integer.valueOf(operationParameters[7]);
        byte expectedReturnStatus = Byte.valueOf(operationParameters[8]);

        LedgerWriteEntryReqBKPOperation lweOperation = new LedgerWriteEntryReqBKPOperation(timeSlot, threadId,
                requestType, newRequest, expectingResponse, extentId, fragmentID, size,
                BKPConstants.LedgerWriteEntryResp, expectedReturnStatus);

        return lweOperation;
    }

    @Override
    public void sendRequest(SocketChannel clientSocketChannel) throws IOException {
        if (newRequest) {
            super.sendRequest(clientSocketChannel);
        }
    }

    @Override
    public void sendPayload(SocketChannel clientSocketChannel) throws IOException {
        if (newRequest) {
            fragArray = new byte[size];
            (new Random()).nextBytes(fragArray);

            putIntToRequest(clientSocketChannel, fragmentID);
            putIntToRequest(clientSocketChannel, size);
            putEntryToRequest(clientSocketChannel, fragArray);
        }
    }

    @Override
    public void receiveResponseAndVerify(SocketChannel clientSocketChannel) throws IOException, OperationException {
        if (expectingResponse) {
            super.receiveResponseAndVerify(clientSocketChannel);
        } else {
            try {
                clientSocketChannel.configureBlocking(false);
                Thread.sleep(WAITTIMEBEFORERESPONSECHECKINSECS * 1000);
                ByteBuffer tempBuff = ByteBuffer.allocate(2);
                clientSocketChannel.read(tempBuff);
                clientSocketChannel.read(tempBuff);
                if (tempBuff.position() != 0) {
                    throw new OperationException(
                            String.format(
                                    "Operation at Timeslot: %d in ThreadId: %s has failed because response is not expected but there is response",
                                    getTimeSlot(), getThreadId()));
                }
            } catch (InterruptedException e) {
                throw new OperationException(
                        String.format(
                                "Operation at Timeslot: %d in ThreadId: %s has failed because it received Interruption while waiting",
                                getTimeSlot(), getThreadId()));
            }
        }
    }

    @Override
    public void catalogBookKeeping() {
        TestScenarioState currentScenario = TestScenarioState.getCurrentTestScenarioState();
        if (newRequest && expectingResponse) {
            if (getExpectedReturnStatus() == BKPConstants.SF_OK) {
                currentScenario.newFragmentAdded(getExtentId(), fragmentID, fragArray);
            }
        } else if (newRequest && (!expectingResponse)) {
            currentScenario.newFragmentInFlight(getExtentId(), fragmentID, fragArray);
        } else if (!newRequest && expectingResponse) {
            if (getExpectedReturnStatus() == BKPConstants.SF_OK) {
                byte[] inFlightFragArray = currentScenario.getInFlightFragment(getExtentId(), fragmentID);
                currentScenario.removeInFlightFragment(getExtentId(), fragmentID);
                currentScenario.newFragmentAdded(getExtentId(), fragmentID, inFlightFragArray);
            }
        }
    }
}
