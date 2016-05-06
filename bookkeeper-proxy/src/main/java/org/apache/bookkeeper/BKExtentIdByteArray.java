package org.apache.bookkeeper;

import java.nio.ByteBuffer;

import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.commons.codec.binary.Hex;

public class BKExtentIdByteArray implements BKExtentId {

    private final byte[] extentId = new byte[BKPConstants.EXTENTID_SIZE];
    private final LedgerIdFormatter ledgerIdFormatter;
    private final int hashCode;

    public BKExtentIdByteArray(ByteBuffer src, LedgerIdFormatter formatter) {
        src.get(extentId);
        this.ledgerIdFormatter = formatter;
        this.hashCode = this.getHashCode();
    }

    public BKExtentIdByteArray(ByteBuffer src) {
        this(src, LedgerIdFormatter.LONG_LEDGERID_FORMATTER);
    }

    public BKExtentIdByteArray(long src, LedgerIdFormatter formatter) {
        ByteBuffer buf = ByteBuffer.allocate(BKPConstants.EXTENTID_SIZE);
        buf.clear();
        buf.putLong(0L);
        buf.putLong(src);
        buf.flip();
        buf.get(extentId);
        this.ledgerIdFormatter = formatter;
        this.hashCode = this.getHashCode();
    }

    public BKExtentIdByteArray(long src) {
        this(src, LedgerIdFormatter.LONG_LEDGERID_FORMATTER);
    }

    private int getHashCode() {
        return new Long(asLong()).hashCode();
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    @Override
    public byte[] asByteArray() {
        return ByteBuffer.wrap(extentId).asReadOnlyBuffer().array();
    }

    @Override
    public ByteBuffer asByteBuffer() {
        return ByteBuffer.wrap(extentId).asReadOnlyBuffer();
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.put(extentId);
    }

    @Override
    public int size() {
        return extentId.length;
    }

    @Override
    public String asHexString() {
        return Hex.encodeHexString(extentId);
    }

    @Override
    public String asString() {
        return ledgerIdFormatter.formatLedgerId(this.asLong());
    }

    @Override
    public String toString() {
        return this.asString();
    }

    @Override
    public long asLong() {
        return ByteBuffer.wrap(extentId, BKPConstants.EXTENTID_SIZE/2, 8).getLong();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof BKExtentIdByteArray) &&
                (this.asLong() == ((BKExtentIdByteArray)obj).asLong());
    }

}
