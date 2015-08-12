package org.apache.bookkeeper;

import java.nio.ByteBuffer;

public class Fragment implements Packet {
	private ByteBuffer buf = null;

	public Fragment(byte[] bytes) {
		buf = ByteBuffer.allocate(bytes.length);
		buf.put(bytes);
	}

	public Fragment(byte header, byte[] payload) {
		buf = ByteBuffer.allocate(payload.length + 1);
		buf.put(header).put(payload);
	}

	//TODO 
	public void setPayload(byte[] payload) {
	}

	// TODO 
	public byte[] getPayload() {
		return null;
	}

	// TODO 
	public void setHeader(byte header) {
		buf = buf.put(0, header);
	}

	public byte getHeader() {
		return buf.get(0);
	}

	public String toString() {
		//TBD: Use stringbuffer
		byte[] b = this.toBytes();
		String str = "";
		for(int i = 0; i < b.length; i++)
			str += Byte.toString(buf.get(i));
		return str;
	}

	public byte[] toBytes() {
//		buf.flip();
		return buf.array();
	}
	
	public void test(int num) throws Exception {
		System.out.println("Test # " + num);
		System.out.println("Limit = " + buf.limit() + " Capacity = "+ buf.capacity());
		System.out.println(this);
		byte[] ret = this.toBytes();
		for(int i = 0; i < ret.length; i++)
			System.out.print(ret[i]);
		System.out.println();
		System.out.println(this.getHeader());
	}
	
	public static void main(String[] args) throws Exception {

		byte h = 22;
		byte[] p = {1,2,3,4,5,6,7,8,9,0};
		Fragment f = new Fragment(h, p);
		f.test(0);
		
		f.setHeader((byte)99);
		f.test(1);

		byte[] p1 = {11,22,33,44};
		f.setPayload(p);
		f.test(2);
	}
}

