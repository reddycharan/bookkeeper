package org.apache.bookkeeper.proxyclient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentAsyncWriteFragmentOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentAsyncWriteStatusOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentCreateOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentDeleteAllOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentDeleteOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentListGetOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentOpenReadOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentOpenRecoverOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentReadCloseOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentReadFragmentOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentStatOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentWriteCloseOperation;
import org.apache.bookkeeper.proxyclient.BKProxyOperation.ExtentWriteFragmentOperation;
import org.apache.bookkeeper.proxyclient.ReturnValue.AsyncWriteStatusReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.CreateExtentReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ExtentListGetReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ExtentStatReturnValue;
import org.apache.bookkeeper.proxyclient.ReturnValue.ReadFragmentReturnValue;
import org.apache.commons.lang.ArrayUtils;

public class BKProxyClient implements AutoCloseable {

    private final String bkProxyHostname;
    private final int bkProxyPort;
    private SocketChannel clientSocketChannel;
    private ByteBuffer proxyClientBuffer;

    /**
     * Initializes a BKProxyClient instance, with which you can make BKProxy operation calls. It creates ByteBuffer of
     * size 1 MB, which is used for both input and output operations. It is not thread-safe.
     * 
     * @param hostname
     *            - hostname of the machine where BKProxy is running
     * @param port
     *            - port on which BKProxy is listening
     */
    public BKProxyClient(String hostname, int port) {
        this.bkProxyHostname = hostname;
        this.bkProxyPort = port;
        proxyClientBuffer = ByteBuffer.allocate(1024 * 1024);
        proxyClientBuffer.order(ByteOrder.nativeOrder());
    }

    /**
     * Initializes a BKProxyClient instance, with which you can make BKProxy operation calls. It is not thread-safe.
     * 
     * @param hostname
     *            - hostname of the machine where BKProxy is running
     * @param port
     *            - port on which BKProxy is listening
     * @param proxyClientBufferSize
     *            - size of the ByteBuffer, which will be used for all the input and output operations
     */
    public BKProxyClient(String hostname, int port, int proxyClientBufferSize) {
        this.bkProxyHostname = hostname;
        this.bkProxyPort = port;
        proxyClientBuffer = ByteBuffer.allocate(proxyClientBufferSize);
        proxyClientBuffer.order(ByteOrder.nativeOrder());
    }

    /**
     * init method needs to be called before making any operation call. It opens the client socketchannel and connects
     * to the BKproxy socket.
     * 
     * @return
     * @throws IOException
     */
    public BKProxyClient init() throws IOException {
        clientSocketChannel = SocketChannel.open();
        clientSocketChannel.connect(new InetSocketAddress(bkProxyHostname, bkProxyPort));
        return this;
    }

    /**
     * close method closes the clientsocketchannel
     */
    @Override
    public void close() throws IOException {
        clientSocketChannel.close();
    }

    /**
     * Makes ExtentCreate BKProxy call with the given extentId
     * 
     * @param extentId
     *            - Id of the extent we want to create
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue createExtent(byte[] extentId) throws IOException {
        ExtentCreateOperation createOperation = new ExtentCreateOperation(extentId, proxyClientBuffer);
        return createOperation.perform(clientSocketChannel);
    }

    /**
     * Makes ExtentCreate BKProxy call with a random extentId
     * 
     * @return - returns CreateExtentReturnValue, which contains the return status and the extentId of the created
     *         extent
     * @throws IOException
     */
    public CreateExtentReturnValue createExtent() throws IOException {
        byte[] precedingZeros = new byte[8];
        // In SDB-BKProxy communication, implicitly ExtentId would be communicated in BIG_ENDIAN byteOrder.
        // Since when SDB sends 128 bit extentID, first 65 bits would be 0 and the positive long value is in
        // last 63 Bits. Which implicitly means that SDB is sending extentID in BIG_ENDIAN byteOrder.
        // In BKProxyWorker, for 'extentBbuf' ByteBuffer we are not setting byteorder explicitly so its byteorder
        // will be BIG_ENDIAN (The order of a newly-created byte buffer is always BIG_ENDIAN.)

        // so here 'randomLongByteBuffer' should be in BIG_ENDIAN and by default ByteBuffer will be in BIG_ENDIAN
        ByteBuffer randomLongByteBuffer = ByteBuffer.allocate(Long.BYTES);
        randomLongByteBuffer.putLong(Math.abs(ThreadLocalRandom.current().nextLong()));
        byte[] extentId = ArrayUtils.addAll(precedingZeros, randomLongByteBuffer.array());
        ExtentCreateOperation createOperation = new ExtentCreateOperation(extentId, proxyClientBuffer);
        ReturnValue retValue = createOperation.perform(clientSocketChannel);
        CreateExtentReturnValue createExtentReturnValue = new CreateExtentReturnValue(retValue.getReturnCode(),
                extentId);
        return createExtentReturnValue;
    }

    /**
     * Makes DeleteExtent BKProxy call with the given extentId
     * 
     * @param extentId
     *            - Id of the extent we want to delete
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue deleteExtent(byte[] extentId) throws IOException {
        ExtentDeleteOperation deleteOperation = new ExtentDeleteOperation(extentId, proxyClientBuffer);
        return deleteOperation.perform(clientSocketChannel);
    }

    /**
     * Makes DeleteAllExtent BKProxy call. It deletes all the extents in the cluster
     * 
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue deleteAllExtents() throws IOException {
        ExtentDeleteAllOperation deleteAllOperation = new ExtentDeleteAllOperation(new byte[16], proxyClientBuffer);
        return deleteAllOperation.perform(clientSocketChannel);
    }

    /**
     * Makes OpenExtentForRead BKProxy call with the given extentId
     * 
     * @param extentId
     *            - Id of the extent for which we want to do OpenExtentForRead
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue openExtentForRead(byte[] extentId) throws IOException {
        ExtentOpenReadOperation openReadOperation = new ExtentOpenReadOperation(extentId, proxyClientBuffer);
        return openReadOperation.perform(clientSocketChannel);
    }

    /**
     * Makes OpenExtentForRecoverRead BKProxy call with the given extentId
     * 
     * @param extentId
     *            - Id of the extent for which we want to do OpenExtentForRecoverRead
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue openExtentForRecoverRead(byte[] extentId) throws IOException {
        ExtentOpenRecoverOperation openRecoverOperation = new ExtentOpenRecoverOperation(extentId, proxyClientBuffer);
        return openRecoverOperation.perform(clientSocketChannel);
    }

    /**
     * Makes ReadCloseExtent BKProxy call with the given extentId
     * 
     * @param extentId
     *            - Id of the extent for which we want to get ReadClose
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue readCloseExtent(byte[] extentId) throws IOException {
        ExtentReadCloseOperation readCloseOperation = new ExtentReadCloseOperation(extentId, proxyClientBuffer);
        return readCloseOperation.perform(clientSocketChannel);
    }

    /**
     * Makes GetExtentStat BKProxy call with the given extentId
     * 
     * @param extentId
     *            - Id of the extent for which we want to get ExtentStat
     * @return - returns ExtentStatReturnValue, which contains the return status and the ExtentStat (extentLength &
     *         extentCTime) of the extent
     * @throws IOException
     */
    public ExtentStatReturnValue getExtentStat(byte[] extentId) throws IOException {
        ExtentStatOperation statOperation = new ExtentStatOperation(extentId, proxyClientBuffer);
        return (ExtentStatReturnValue) statOperation.perform(clientSocketChannel);
    }

    /**
     * Makes WriteCloseExtent BKProxy call with the given extentId
     * 
     * @param extentId
     *            - Id of the extent we want to WriteClose
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue writeCloseExtent(byte[] extentId) throws IOException {
        ExtentWriteCloseOperation writeCloseOperation = new ExtentWriteCloseOperation(extentId, proxyClientBuffer);
        return writeCloseOperation.perform(clientSocketChannel);
    }

    /**
     * 
     * Makes WriteFragment BKProxy call with the given extentId, fragmentId and fragmentData
     * 
     * @param extentId
     *            - id of the extent
     * @param fragmentId
     *            - id of the fragment
     * @param fragmentData
     *            - fragment data byte array
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue writeFragment(byte[] extentId, int fragmentId, byte[] fragmentData) throws IOException {
        ExtentWriteFragmentOperation writeFragmentOperation = new ExtentWriteFragmentOperation(extentId, fragmentId,
                fragmentData, proxyClientBuffer);
        return writeFragmentOperation.perform(clientSocketChannel);
    }

    /**
     * Makes asyncWriteFragment BKProxy call with the given extentId, fragmentId and fragmentData
     * 
     * @param extentId
     *            - id of the extent
     * @param fragmentId
     *            - id of the fragment
     * @param fragmentData
     *            - fragment data byte array
     * @return - returns the ReturnValue, which wraps the code of the return status
     * @throws IOException
     */
    public ReturnValue asyncWriteFragment(byte[] extentId, int fragmentId, byte[] fragmentData) throws IOException {
        ExtentAsyncWriteFragmentOperation asyncWriteFragmentOperation = new ExtentAsyncWriteFragmentOperation(extentId,
                fragmentId, fragmentData, proxyClientBuffer);
        return asyncWriteFragmentOperation.perform(clientSocketChannel);
    }

    /**
     * Makes asyncWriteFragment BKProxy call with the given extentId, fragmentId and timeout
     * 
     * @param extentId
     *            - id of the extent
     * @param fragmentId
     *            - id of the fragment
     * @param timeout
     *            - timeout period
     * @return - returns AsyncWriteStatusReturnValue, which contains the return status, asyncWriteStatus and
     *         completionTime
     * @throws IOException
     */
    public AsyncWriteStatusReturnValue asyncWriteStatus(byte[] extentId, int fragmentId, int timeout)
            throws IOException {
        ExtentAsyncWriteStatusOperation asyncWriteStatusOperation = new ExtentAsyncWriteStatusOperation(extentId,
                fragmentId, timeout, proxyClientBuffer);
        return (AsyncWriteStatusReturnValue) asyncWriteStatusOperation.perform(clientSocketChannel);
    }

    /**
     * Makes readFragment BKProxy call with the given extentId, fragmentId and 1 MB size (as expected max size of the
     * fragment)
     * 
     * @param extentId
     *            - id of the extent
     * @param fragmentId
     *            - id of the fragment
     * @return - returns ReadFragmentReturnValue, which contains the return status, fragmentSize and fragmentData byte
     *         array
     * @throws IOException
     */
    public ReadFragmentReturnValue readFragment(byte[] extentId, int fragmentId) throws IOException {
        return readFragment(extentId, fragmentId, 1024 * 1024);
    }

    /**
     * Makes readFragment BKProxy call with the given extentId, fragmentId and expected max size of the fragment
     * 
     * @param extentId
     *            - id of the extent
     * @param fragmentId
     *            - id of the fragment
     * @param size
     *            - expected max size of the fragment
     * @return - returns ReadFragmentReturnValue, which contains the return status, fragmentSize and fragmentData byte
     *         array
     * @throws IOException
     */
    public ReadFragmentReturnValue readFragment(byte[] extentId, int fragmentId, int size) throws IOException {
        ExtentReadFragmentOperation readFragmentOperation = new ExtentReadFragmentOperation(extentId, fragmentId, size,
                proxyClientBuffer);
        return (ReadFragmentReturnValue) readFragmentOperation.perform(clientSocketChannel);
    }

    /**
     * Makes getExtentsList BKProxy call
     * 
     * @return - returns ExtentListGetReturnValue, which contains the return status and extentIdsList
     * @throws IOException
     */
    public ExtentListGetReturnValue getExtentsList() throws IOException {
        ExtentListGetOperation extentListGetOperation = new ExtentListGetOperation(new byte[16], proxyClientBuffer);
        return (ExtentListGetReturnValue) extentListGetOperation.perform(clientSocketChannel);
    }
}
