package org.apache.bookkeeper;

import java.nio.ByteBuffer;

public interface BKExtentId {
    /**
     * Read the extentId out of the supplied buffer. Buffer should have
     * enough readable bytes for an extentId.
     *
     * @param buffer the buffer containing the marshaled extentId
     */
    void read(ByteBuffer buffer);

    /**
     * Write the extentId into supplied buffer. Buffer should have
     * enough space for an extentId.
     *
     * @param buffer the buffer where the extentId is to be marshaled
     */
    void write(ByteBuffer buffer);
    
    /**
     * Get the extentId as a byte array.
     *
     * @return extentId in byte array
     */
    byte[] asByteArray();
    
    /**
     * Get the size of the extentId
     *
     * @return the size
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

    /**
     * Copy and return a new instance
     *
     * @return same object.
     */
    BKExtentId copy();

}
