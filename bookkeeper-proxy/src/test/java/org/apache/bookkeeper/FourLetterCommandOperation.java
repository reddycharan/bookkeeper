package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class FourLetterCommandOperation extends BKPOperationExtension {

    private final String fourLetterCommand;
    private final String expectedResponse;
    private static final int RESPONSEBUFFERSIZE = 10;

    public FourLetterCommandOperation(short protocolVersion, int timeSlot, String threadId, byte requestType,
            byte[] extentId, byte responseType, byte expectedReturnStatus, String fourLetterCommand,
            String expectedResponse) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.fourLetterCommand = fourLetterCommand;
        this.expectedResponse = expectedResponse;
    }

    public static FourLetterCommandOperation createFourLetterCommandOperation(short protocolVersion,
            String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != Operation.FourLettersReq) {
            throw new IllegalArgumentException("Expected FourLettersReq in the Operation Definition");
        }
        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        String fourLetterCommand = operationParameters[3];
        String expectedResponse = operationParameters[4];
        FourLetterCommandOperation fourLetterCommandOperation = new FourLetterCommandOperation(protocolVersion,
                timeSlot, threadId, requestType, null, (byte) 0, (byte) 0, fourLetterCommand, expectedResponse);
        return fourLetterCommandOperation;
    }

    public void sendRequest(SocketChannel clientSocketChannel) throws IOException {
        ByteBuffer req = ByteBuffer.wrap(fourLetterCommand.getBytes());
        req.order(ByteOrder.nativeOrder());

        while (req.hasRemaining()) {
            clientSocketChannel.write(req);
        }
    }

    public void receiveResponseAndVerify(SocketChannel clientSocketChannel) throws IOException, OperationException {
        ByteBuffer resp = ByteBuffer.allocate(RESPONSEBUFFERSIZE);
        int totalBytesRead = 0;
        int bytesRead;
        while (true) {
            try {
                bytesRead = clientSocketChannel.read(resp);
                if (bytesRead < 0) {
                    break;
                }
                totalBytesRead += bytesRead;
            } catch (IOException e) {
                break;
            }
        }
        String actualResponseString = (new String(resp.array())).substring(0, totalBytesRead);

        if (!expectedResponse.equals(actualResponseString)) {
            throw new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected response: %s, whereas the expected value is: %s",
                    getTimeSlot(), getThreadId(), actualResponseString, expectedResponse));
        }
    }
}
