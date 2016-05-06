package org.apache.bookkeeper;

import java.nio.ByteBuffer;

import org.apache.bookkeeper.util.LedgerIdFormatter;

public class BKExtentIdByteArrayFactory extends BKExtentIdFactory {

    @Override
    public BKExtentId build(ByteBuffer src) {
        return new BKExtentIdByteArray(src);
    }

    @Override
    public BKExtentId build(ByteBuffer src, LedgerIdFormatter formatter) {
        return new BKExtentIdByteArray(src, formatter);
    }

    @Override
    public BKExtentId build(long src) {
        return new BKExtentIdByteArray(src);
    }

    @Override
    public BKExtentId build(long src, LedgerIdFormatter formatter) {
        return new BKExtentIdByteArray(src, formatter);
    }

}
