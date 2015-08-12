package org.apache.bookkeeper;
public interface Packet {
	public String toString();
	public byte[] toBytes();
	public void setPayload(byte[] payload);
	public byte[] getPayload();
	public void setHeader(byte header);
	public byte getHeader();
}
