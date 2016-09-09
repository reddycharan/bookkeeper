package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public abstract class BKPOperation extends AbstractOperation {
    private short protocolVersion;
    private byte requestType;
    private byte responseType;
    private byte expectedReturnStatus;
    private byte[] extentId;    
    public static final byte[] pad = new byte[5];
    
    public BKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        this.protocolVersion = protocolVersion;
        this.timeSlot = timeSlot;
        this.threadId = threadId;
        TestScenarioState currentTestScenario = TestScenarioState.getCurrentTestScenarioState();
        if (currentTestScenario.getBKPClientThread(threadId) == null) {
            throw new IllegalArgumentException("ThreadId: " + threadId + " is not defined");
        }
        this.requestType = requestType;
        this.extentId = extentId;
        this.responseType = responseType;
        this.expectedReturnStatus = expectedReturnStatus;
    }

    public byte getRequestType() {
        return requestType;
    }

    public byte getResponseType() {
        return responseType;
    }

    public byte getExpectedReturnStatus() {
        return expectedReturnStatus;
    }

    public byte[] getExtentId() {
        return extentId;
    }

    public static BKPOperation build(final short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        BKPOperation bkpOperation = null;
        switch (requestType) {
        case BKPConstants.LedgerStatReq:
            bkpOperation = LedgerStatReqBKPOperation.createLedgerStatReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerDeleteReq:
            bkpOperation = LedgerDeleteReqBKPOperation.createLedgerDeleteReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerCreateReq:
            bkpOperation = LedgerCreateReqBKPOperation.createLedgerCreateReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerWriteCloseReq:
            bkpOperation = LedgerWriteCloseReqBKPOperation.createLedgerWriteCloseReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerOpenRecoverReq:
            bkpOperation = LedgerOpenRecoverReqBKPOperation.createLedgerOpenRecoverReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerOpenReadReq:
            bkpOperation = LedgerOpenReadReqBKPOperation.createLedgerOpenReadReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerWriteEntryReq:
            bkpOperation = LedgerWriteEntryReqBKPOperation.createLedgerWriteEntryReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerReadEntryReq:
            bkpOperation = LedgerReadEntryReqBKPOperation.createLedgerReadEntryReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerReadCloseReq:
            bkpOperation = LedgerReadCloseReqBKPOperation.createLedgerReadCloseReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerListGetReq:
            bkpOperation = LedgerListGetReqBKPOperation.createLedgerListGetReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerDeleteAllReq:
            bkpOperation = LedgerDeleteAllReqBKPOperation.createLedgerDeleteAllReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerAsyncWriteEntryReq:
            bkpOperation = LedgerAsyncWriteEntryReqBKPOperation.createLedgerAsyncWriteEntryReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case BKPConstants.LedgerAsyncWriteStatusReq:
            bkpOperation = LedgerAsyncWriteStatusReqBKPOperation.createLedgerAsyncWriteStatusReqBKPOperation(protocolVersion, operationDefinition);
            break;
        case Operation.FourLettersReq:
            bkpOperation = FourLetterCommandOperation.createFourLetterCommandOperation(protocolVersion, operationDefinition);
            break;
        }
        return bkpOperation;
    }

    public void sendRequest(SocketChannel clientSocketChannel) throws IOException {
        ByteBuffer req = ByteBuffer.allocate(BKPConstants.GENERIC_REQ_SIZE);
        req.order(ByteOrder.nativeOrder());
        req.putShort(protocolVersion);
        req.put(getRequestType());
        req.put(getExtentId());
        req.put(pad);
        req.flip();

        while (req.hasRemaining()) {
            clientSocketChannel.write(req);
        }
    }

    public void receiveResponseAndVerify(SocketChannel clientSocketChannel) throws IOException, OperationException {
        ByteBuffer resp = ByteBuffer.allocate(2);
        resp.order(ByteOrder.nativeOrder());
        while (resp.hasRemaining()) {
            clientSocketChannel.read(resp);
        }
        resp.flip();
        byte actualResponseType = resp.get();
        byte actualReturnStatus = resp.get();
        if (actualResponseType != getResponseType()) {
            throw new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected responseType: %d, whereas the expected value is: %d",
                    getTimeSlot(), getThreadId(), actualResponseType, getResponseType()));
        }

        if (actualReturnStatus != getExpectedReturnStatus()) {
            throw new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected ReturnStatus: %d, whereas the expected value is: %d",
                    getTimeSlot(), getThreadId(), actualReturnStatus, getExpectedReturnStatus()));
        }
    }

    public abstract void sendPayload(SocketChannel clientSocketChannel) throws IOException;

    public abstract void receivePayloadAndVerify(SocketChannel clientSocketChannel)
            throws OperationException, IOException;

    public abstract void catalogBookKeeping();
    
    @Override
    public void perform(Object ctx) {
        try {
            doPrePerformSleep();
            SocketChannel clientSocketChannel = (SocketChannel) ctx;
            sendRequest(clientSocketChannel);
            sendPayload(clientSocketChannel);
            receiveResponseAndVerify(clientSocketChannel);
            receivePayloadAndVerify(clientSocketChannel);
            catalogBookKeeping();
        } catch (OperationException e) {            
            isOperationFailed = true;
            operationException = e;
        } catch (IOException e) {            
            isOperationFailed = true;
            operationException = new IOException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected IOException: %s",
                    getTimeSlot(), getThreadId(), e), e);
        }
    }

    public void getIntFromResponseAndVerify(SocketChannel clientSocketChannel, int expectedValue, String paramName)
            throws OperationException, IOException {
        ByteBuffer nextIntResp = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        nextIntResp.order(ByteOrder.nativeOrder());
        while (nextIntResp.hasRemaining()) {
            clientSocketChannel.read(nextIntResp);
        }
        nextIntResp.flip();
        int actualvalue = nextIntResp.getInt();
        if (actualvalue != expectedValue) {
            throw new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected %s : %d, whereas the expected value is %d",
                    getTimeSlot(), getThreadId(), paramName, actualvalue, expectedValue));
        }
    }

    public void getLongFromResponseAndVerify(SocketChannel clientSocketChannel, long expectedValue, String paramName)
            throws OperationException, IOException {
        ByteBuffer nextLongResp = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
        nextLongResp.order(ByteOrder.nativeOrder());
        while (nextLongResp.hasRemaining()) {
            clientSocketChannel.read(nextLongResp);
        }
        nextLongResp.flip();
        long actualvalue = nextLongResp.getLong();
        if (actualvalue != expectedValue) {
            throw new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected %s : %d, whereas the expected value is %d",
                    getTimeSlot(), getThreadId(), paramName, actualvalue, expectedValue));
        }
    }
    
    public void getEntryFromResponseAndVerify(SocketChannel clientSocketChannel, byte[] expectedByteArray,
            String paramName) throws IOException, OperationException {
        ByteBuffer actualTrailer = ByteBuffer.allocate(expectedByteArray.length);
        actualTrailer.order(ByteOrder.nativeOrder());
        while (actualTrailer.hasRemaining()) {
            clientSocketChannel.read(actualTrailer);
        }
        actualTrailer.flip();
        byte[] actualByteArray = actualTrailer.array();
        if (!Arrays.equals(actualByteArray, expectedByteArray)) {
            throw new OperationException(
                    String.format("Operation at Timeslot: %d in ThreadId: %s has failed because of unequal %s",
                            getTimeSlot(), getThreadId(), paramName));
        }
    }

    public void putIntToRequest(SocketChannel clientSocketChannel, int param) throws IOException {
        ByteBuffer paramBuff = ByteBuffer.allocate(Integer.SIZE / 8);
        paramBuff.order(ByteOrder.nativeOrder());
        paramBuff.putInt(param);
        paramBuff.flip();

        while (paramBuff.hasRemaining()) {
            clientSocketChannel.write(paramBuff);
        }
    }

    public void putEntryToRequest(SocketChannel clientSocketChannel, byte[] fragArray) throws IOException {
        ByteBuffer fragBuffer = ByteBuffer.allocate(fragArray.length);
        fragBuffer.order(ByteOrder.nativeOrder());
        fragBuffer.put(fragArray);
        fragBuffer.flip();
        while (fragBuffer.hasRemaining()) {
            clientSocketChannel.write(fragBuffer);
        }
    }
}
