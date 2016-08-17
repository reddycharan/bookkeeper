package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class LedgerAsyncWriteStatusReqBKPOperation extends BKPOperationExtension {

    private int fragmentID;
    private int timeout;
    private static final int MAXWAITTIME_FOR_ASYNCWRITESTATUS = 10 * 1000;
    public static final String INFINITE_TIMEOUT = "INFINITE";

    public LedgerAsyncWriteStatusReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType,
            byte[] extentId, int fragmentID, int timeout, byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.fragmentID = fragmentID;
        this.timeout = timeout;
    }

    public static LedgerAsyncWriteStatusReqBKPOperation createLedgerAsyncWriteStatusReqBKPOperation(
            short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerAsyncWriteStatusReq) {
            throw new IllegalArgumentException("Expected LedgerAsyncWriteStatusReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];

        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);
        int fragmentID = Integer.valueOf(operationParameters[4]);
        int timeout = 0;
        try {
            timeout = Integer.valueOf(operationParameters[5]);
        } catch (NumberFormatException e) {
            if (operationParameters[5].equals(INFINITE_TIMEOUT)) {
                timeout = -1;
            } else {
                throw new IllegalArgumentException(
                        "Received IllegalArgument - " + operationParameters[5] + " for LedgerAsyncWriteStatusReqBKPOperation timeout value ");
            }
        }
        byte expectedReturnStatus = Byte.valueOf(operationParameters[6]);

        LedgerAsyncWriteStatusReqBKPOperation lawsOperation = new LedgerAsyncWriteStatusReqBKPOperation(protocolVersion,
                timeSlot, threadId, requestType, extentId, fragmentID, timeout, BKPConstants.LedgerAsyncWriteStatusResp,
                expectedReturnStatus);

        return lawsOperation;
    }

    @Override
    public void preSetup(TestScenarioState state) {
        if (this.timeout >= 0) {
            state.setAdditionalTimeoutWaitTime(Math.max(state.getAdditionalTimeoutWaitTime(), timeout));
        } else {
            state.setAdditionalTimeoutWaitTime(
                    Math.max(state.getAdditionalTimeoutWaitTime(), MAXWAITTIME_FOR_ASYNCWRITESTATUS));
        }
    };

    @Override
    public void sendPayload(SocketChannel clientSocketChannel) throws IOException {
        putIntToRequest(clientSocketChannel, fragmentID);
        putIntToRequest(clientSocketChannel, timeout);
    }

    @Override
    public void catalogBookKeeping() {
        TestScenarioState currentScenario = TestScenarioState.getCurrentTestScenarioState();
        if (getExpectedReturnStatus() == BKPConstants.SF_OK) {
            byte[] inFlightFragArray = currentScenario.getInFlightFragment(getExtentId(), fragmentID);
            currentScenario.removeInFlightFragment(getExtentId(), fragmentID);
            currentScenario.newFragmentAdded(getExtentId(), fragmentID, inFlightFragArray);
        }
    }
}
