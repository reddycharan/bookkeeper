package org.apache.bookkeeper;

import java.nio.ByteBuffer;

import org.apache.bookkeeper.util.LedgerIdFormatter;

public abstract class BKExtentIdFactory {

    /**
     * Build an extentId out of a ByteBuffer. Buffer should have
     * enough readable bytes for an extentId.
     *
     * @param src the ByteBuffer containing the marshaled extentId
     */
    public abstract BKExtentId build(ByteBuffer src);

    /**
     * Build an extentId out of a ByteBuffer. Buffer should have
     * enough readable bytes for an extentId.
     *
     * @param src the ByteBuffer containing the marshaled extentId
     */
    public abstract BKExtentId build(ByteBuffer src, LedgerIdFormatter formatter);

    /**
     * Build an extentId out of a long. The extentId will be padded
     * with leading 8 bytes containing zero.
     *
     * @param src the long containing the marshaled extentId
     */
    public abstract BKExtentId build(long src);

    /**
     * Build an extentId out of a long. The extentId will be padded
     * with leading 8 bytes containing zero.
     *
     * @param src the long containing the marshaled extentId
     */
    public abstract BKExtentId build(long src, LedgerIdFormatter formatter);

}
