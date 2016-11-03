package org.apache.bookkeeper.proxyclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.BKPConstants;
import org.apache.bookkeeper.proxyclient.ReturnValue.AsyncWriteStatusReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ExtentListGetReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ExtentStatReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ReadFragmentReturnValue;

public abstract class BKProxyOperation {

    private byte requestType;
    private byte[] extentId;
    private ByteBuffer proxyClientBuffer;
    public static final byte[] pad = new byte[5];

    public BKProxyOperation(byte requestType, byte[] extentId, ByteBuffer proxyClientBuffer) {
        this.requestType = requestType;
        this.extentId = extentId;
        this.proxyClientBuffer = proxyClientBuffer;
    }

    public byte[] getExtentId() {
        return extentId;
    }

    public byte getRequestType() {
        return requestType;
    }

    public byte getResponseType() {
        return BKPConstants.getRespId(this.requestType);
    }

    private void sendRequest(SocketChannel clientSocketChannel) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(BKPConstants.GENERIC_REQ_SIZE);
        proxyClientBuffer.putShort(BKPConstants.SFS_CURRENT_VERSION);
        proxyClientBuffer.put(getRequestType());
        proxyClientBuffer.put(getExtentId());
        proxyClientBuffer.put(pad);
        proxyClientBuffer.flip();

        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.write(proxyClientBuffer);
        }
    }

    abstract void sendPayload(SocketChannel clientSocketChannel) throws IOException;

    byte receiveResponse(SocketChannel clientSocketChannel) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(2);

        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.read(proxyClientBuffer);
        }
        proxyClientBuffer.flip();
        byte actualResponseType = proxyClientBuffer.get();
        byte actualReturnStatus = proxyClientBuffer.get();

        if (actualResponseType != getResponseType()) {
            throw new RuntimeException(String.format(
                    "Operation has failed because of unexpected responseType: %d, whereas the expected value is: %d",
                    actualResponseType, getResponseType()));
        }
        return actualReturnStatus;
    }

    abstract ReturnValue receivePayload(SocketChannel clientSocketChannel, byte returnCode) throws IOException;

    public ReturnValue perform(SocketChannel clientSocketChannel) throws IOException {
        sendRequest(clientSocketChannel);
        sendPayload(clientSocketChannel);
        byte returnCode = receiveResponse(clientSocketChannel);
        return receivePayload(clientSocketChannel, returnCode);
    }

    protected byte getByteFromResponse(SocketChannel clientSocketChannel) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(1);

        proxyClientBuffer.order(ByteOrder.nativeOrder());
        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.read(proxyClientBuffer);
        }
        proxyClientBuffer.flip();
        return proxyClientBuffer.get();
    }

    protected int getIntFromResponse(SocketChannel clientSocketChannel) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(Integer.SIZE / Byte.SIZE);

        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.read(proxyClientBuffer);
        }
        proxyClientBuffer.flip();
        return proxyClientBuffer.getInt();
    }

    protected long getLongFromResponse(SocketChannel clientSocketChannel) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(Long.SIZE / Byte.SIZE);

        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.read(proxyClientBuffer);
        }
        proxyClientBuffer.flip();
        return proxyClientBuffer.getLong();
    }

    protected byte[] getExtentIdFromResponse(SocketChannel clientSocketChannel) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(16);

        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.read(proxyClientBuffer);
        }
        proxyClientBuffer.flip();

        byte[] extentIdArray = new byte[16];
        proxyClientBuffer.get(extentIdArray);
        return extentIdArray;
    }

    protected byte[] getFragmentFromResponse(SocketChannel clientSocketChannel, int fragmentLength) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(fragmentLength);

        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.read(proxyClientBuffer);
        }
        proxyClientBuffer.flip();

        byte[] fragmentArray = new byte[proxyClientBuffer.remaining()];
        proxyClientBuffer.get(fragmentArray);
        return fragmentArray;
    }

    protected void putIntToRequest(SocketChannel clientSocketChannel, int param) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(Integer.SIZE / Byte.SIZE);
        proxyClientBuffer.putInt(param);
        proxyClientBuffer.flip();

        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.write(proxyClientBuffer);
        }
    }

    protected void putFragmentToRequest(SocketChannel clientSocketChannel, byte[] fragmentArray) throws IOException {
        proxyClientBuffer.rewind();
        proxyClientBuffer.limit(fragmentArray.length);
        proxyClientBuffer.put(fragmentArray);
        proxyClientBuffer.flip();
        while (proxyClientBuffer.hasRemaining()) {
            clientSocketChannel.write(proxyClientBuffer);
        }
    }

    public static abstract class BKProxyOperationAdapter extends BKProxyOperation {

        public BKProxyOperationAdapter(byte requestType, byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(requestType, extentId, proxyClientBuffer);
        }

        @Override
        void sendPayload(SocketChannel clientSocketChannel) throws IOException {
        };

        ReturnValue receivePayload(SocketChannel clientSocketChannel, byte returnCode) throws IOException {
            return new ReturnValue(returnCode);
        };
    }

    static class ExtentCreateOperation extends BKProxyOperationAdapter {
        public ExtentCreateOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerCreateReq, extentId, proxyClientBuffer);
        }
    }

    static class ExtentDeleteOperation extends BKProxyOperationAdapter {
        public ExtentDeleteOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerDeleteReq, extentId, proxyClientBuffer);
        }
    }

    static class ExtentDeleteAllOperation extends BKProxyOperationAdapter {
        public ExtentDeleteAllOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerDeleteAllReq, extentId, proxyClientBuffer);
        }
    }

    static class ExtentOpenReadOperation extends BKProxyOperationAdapter {
        public ExtentOpenReadOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerOpenReadReq, extentId, proxyClientBuffer);
        }
    }

    static class ExtentOpenRecoverOperation extends BKProxyOperationAdapter {
        public ExtentOpenRecoverOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerOpenRecoverReq, extentId, proxyClientBuffer);
        }
    }

    static class ExtentReadCloseOperation extends BKProxyOperationAdapter {
        public ExtentReadCloseOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerReadCloseReq, extentId, proxyClientBuffer);
        }
    }

    static class ExtentWriteCloseOperation extends BKProxyOperationAdapter {
        public ExtentWriteCloseOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerWriteCloseReq, extentId, proxyClientBuffer);
        }
    }

    static class ExtentWriteFragmentOperation extends BKProxyOperationAdapter {
        private int fragmentId;
        private byte[] fragmentData;

        public ExtentWriteFragmentOperation(byte[] extentId, int fragmentId, byte[] fragmentData,
                ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerWriteEntryReq, extentId, proxyClientBuffer);
            this.fragmentId = fragmentId;
            this.fragmentData = fragmentData;
        }

        @Override
        void sendPayload(SocketChannel clientSocketChannel) throws IOException {
            putIntToRequest(clientSocketChannel, fragmentId);
            putIntToRequest(clientSocketChannel, fragmentData.length);
            putFragmentToRequest(clientSocketChannel, fragmentData);
        };
    }

    static class ExtentAsyncWriteFragmentOperation extends BKProxyOperationAdapter {
        private int fragmentId;
        private byte[] fragmentData;

        public ExtentAsyncWriteFragmentOperation(byte[] extentId, int fragmentId, byte[] fragmentData,
                ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerAsyncWriteEntryReq, extentId, proxyClientBuffer);
            this.fragmentId = fragmentId;
            this.fragmentData = fragmentData;
        }

        @Override
        void sendPayload(SocketChannel clientSocketChannel) throws IOException {
            putIntToRequest(clientSocketChannel, fragmentId);
            putIntToRequest(clientSocketChannel, fragmentData.length);
            putFragmentToRequest(clientSocketChannel, fragmentData);
        };
    }

    static class ExtentAsyncWriteStatusOperation extends BKProxyOperationAdapter {
        private int fragmentId;
        private int timeout;

        public ExtentAsyncWriteStatusOperation(byte[] extentId, int fragmentId, int timeout,
                ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerAsyncWriteStatusReq, extentId, proxyClientBuffer);
            this.fragmentId = fragmentId;
            this.timeout = timeout;
        }

        @Override
        void sendPayload(SocketChannel clientSocketChannel) throws IOException {
            putIntToRequest(clientSocketChannel, fragmentId);
            putIntToRequest(clientSocketChannel, timeout);
        };

        @Override
        ReturnValue receivePayload(SocketChannel clientSocketChannel, byte returnCode) throws IOException {
            AsyncWriteStatusReturnValue asyncWriteStatusReturnValue = new AsyncWriteStatusReturnValue(returnCode);
            if (returnCode == BKPConstants.SF_OK) {
                asyncWriteStatusReturnValue.setAsyncWriteStatus(getByteFromResponse(clientSocketChannel));
                asyncWriteStatusReturnValue.setCompletionTime(getLongFromResponse(clientSocketChannel));
            }
            return asyncWriteStatusReturnValue;
        };
    }

    static class ExtentReadFragmentOperation extends BKProxyOperationAdapter {
        private int fragmentId;
        private int size;

        public ExtentReadFragmentOperation(byte[] extentId, int fragmentId, int size, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerReadEntryReq, extentId, proxyClientBuffer);
            this.fragmentId = fragmentId;
            this.size = size;
        }

        @Override
        void sendPayload(SocketChannel clientSocketChannel) throws IOException {
            putIntToRequest(clientSocketChannel, fragmentId);
            putIntToRequest(clientSocketChannel, size);
        };

        @Override
        ReturnValue receivePayload(SocketChannel clientSocketChannel, byte returnCode) throws IOException {
            ReadFragmentReturnValue readReturnValue = new ReadFragmentReturnValue(returnCode);
            if (returnCode == BKPConstants.SF_OK) {
                readReturnValue.setFragmentSize(getIntFromResponse(clientSocketChannel));
                readReturnValue.setFragmentData(
                        getFragmentFromResponse(clientSocketChannel, readReturnValue.getFragmentSize()));
            }
            return readReturnValue;
        };
    }

    static class ExtentStatOperation extends BKProxyOperationAdapter {
        public ExtentStatOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerStatReq, extentId, proxyClientBuffer);
        }

        @Override
        ReturnValue receivePayload(SocketChannel clientSocketChannel, byte returnCode) throws IOException {
            ExtentStatReturnValue statReturnValue = new ExtentStatReturnValue(returnCode);
            if (returnCode == BKPConstants.SF_OK) {
                statReturnValue.setExtentLength(getLongFromResponse(clientSocketChannel));
                statReturnValue.setExtentCTime(getLongFromResponse(clientSocketChannel));
            }
            return statReturnValue;
        };
    }

    static class ExtentListGetOperation extends BKProxyOperationAdapter {
        public ExtentListGetOperation(byte[] extentId, ByteBuffer proxyClientBuffer) {
            super(BKPConstants.LedgerListGetReq, extentId, proxyClientBuffer);
        }

        @Override
        ReturnValue receivePayload(SocketChannel clientSocketChannel, byte returnCode) throws IOException {
            ExtentListGetReturnValue extentListGetReturnValue = new ExtentListGetReturnValue(returnCode);
            if (returnCode == BKPConstants.SF_OK) {
                List<byte[]> extentIdsList = new ArrayList<byte[]>();
                int thisBatchSize;
                while ((thisBatchSize = getIntFromResponse(clientSocketChannel)) != 0) {
                    while (thisBatchSize-- > 0) {
                        extentIdsList.add(getExtentIdFromResponse(clientSocketChannel));
                    }
                    receiveResponse(clientSocketChannel);
                }
                extentListGetReturnValue.setExtentIdsList(extentIdsList);
            }
            return extentListGetReturnValue;
        };
    }

}
