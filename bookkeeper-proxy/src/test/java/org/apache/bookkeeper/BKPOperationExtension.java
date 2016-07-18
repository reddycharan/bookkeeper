package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public abstract class BKPOperationExtension extends BKPOperation {

    public BKPOperationExtension(short protocolVersion, int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
    }

    @Override
    public void sendPayload(SocketChannel clientSocketChannel) throws IOException {
        // do nothing
    }

    @Override
    public void receivePayloadAndVerify(SocketChannel clientSocketChannel) throws OperationException, IOException {
        // do nothing
    }

    @Override
    public void catalogBookKeeping() {
        // do nothing
    }
}
