package org.apache.bookkeeper;

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Hex;

public class BKExtentIdByteArray implements BKExtentId {

    private final byte[] extentId;

    public BKExtentIdByteArray() {
        extentId = new byte[BKPConstants.EXTENTID_SIZE];
    }

    public BKExtentIdByteArray copy() {
        BKExtentIdByteArray cExtentId = new BKExtentIdByteArray();
        System.arraycopy(extentId, 0, cExtentId.extentId, 0, extentId.length);
        return cExtentId;
    }

    @Override
    public byte[] asByteArray() {
        return extentId;
    }

    @Override
    public void read(ByteBuffer buffer) {
        buffer.get(extentId);
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
        return new String(extentId, 0, extentId.length - 1);
    }
    
    @Override
    public long asLong() {
        return ByteBuffer.wrap(extentId, BKPConstants.EXTENTID_SIZE/2, 8).getLong();
    }
    
    @Override
    public int hashCode() {
        return new Long(asLong()).hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        return (obj instanceof BKExtentIdByteArray) &&
                new Long(this.asLong()).equals(((BKExtentIdByteArray)obj).asLong());
    }

}
