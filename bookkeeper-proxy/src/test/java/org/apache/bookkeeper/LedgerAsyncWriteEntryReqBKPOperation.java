package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;

public class LedgerAsyncWriteEntryReqBKPOperation extends BKPOperationExtension {

    private int fragmentID;
    private int size;
    private byte[] fragArray;

    public LedgerAsyncWriteEntryReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType,
            byte[] extentId, int fragmentID, int size, byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.fragmentID = fragmentID;
        this.size = size;
    }

    public int getFragmentID() {
        return fragmentID;
    }

    public int getSize() {
        return size;
    }

    public static LedgerAsyncWriteEntryReqBKPOperation createLedgerAsyncWriteEntryReqBKPOperation(short protocolVersion,
            String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerAsyncWriteEntryReq) {
            throw new IllegalArgumentException("Expected LedgerAsyncWriteEntryReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];

        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);
        int fragmentID = Integer.valueOf(operationParameters[4]);
        int size = Integer.valueOf(operationParameters[5]);
        byte expectedReturnStatus = Byte.valueOf(operationParameters[6]);

        LedgerAsyncWriteEntryReqBKPOperation laweOperation = new LedgerAsyncWriteEntryReqBKPOperation(protocolVersion,
                timeSlot, threadId, requestType, extentId, fragmentID, size, BKPConstants.LedgerAsyncWriteEntryResp,
                expectedReturnStatus);

        return laweOperation;
    }

    @Override
    public void sendPayload(SocketChannel clientSocketChannel) throws IOException {
        fragArray = new byte[size];
        (new Random()).nextBytes(fragArray);

        putIntToRequest(clientSocketChannel, fragmentID);
        putIntToRequest(clientSocketChannel, size);
        putEntryToRequest(clientSocketChannel, fragArray);
    }

    @Override
    public void catalogBookKeeping() {
        TestScenarioState currentScenario = TestScenarioState.getCurrentTestScenarioState();
        currentScenario.newFragmentInFlight(getExtentId(), fragmentID, fragArray);
    }
}
