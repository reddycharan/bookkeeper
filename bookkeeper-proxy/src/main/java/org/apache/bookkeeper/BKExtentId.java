package org.apache.bookkeeper;

import java.nio.ByteBuffer;

public interface BKExtentId {

    /**
     * Write the extentId into supplied buffer. Buffer should have
     * enough space for an extentId.
     *
     * @param dst the buffer where the extentId is to be marshaled
     */
    void write(ByteBuffer dst);

    /**
     * Get the extentId as a byte array.
     *
     * @return extentId in byte array
     */
    byte[] asByteArray();

    /**
     * Get the extentId as a ByteBuffer.
     *
     * @return extentId in ByteBuffer
     */
    ByteBuffer asByteBuffer();

    /**
     * Get the number of bytes in the extentId when returned as
     * a byte array.
     *
     * @return the number of bytes
     */
    int size();

    /**
     * Get the extentId as a hexadecimal string.
     *
     * @return extentId in hex format
     */
    String asHexString();

    /**
     * Get the extentId as a string.
     *
     * @return the extentId as a string
     */
    String asString();

    /**
     * Get the extentId as a long.
     *
     * @return the extentId as a long
     */
    long asLong();

}
