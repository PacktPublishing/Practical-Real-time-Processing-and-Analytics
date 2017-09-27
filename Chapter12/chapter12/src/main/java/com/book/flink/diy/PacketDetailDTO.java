package com.book.flink.diy;

import java.io.Serializable;

public class PacketDetailDTO implements Serializable {

	private static final long serialVersionUID = 9148607866335518739L;
	private long phoneNumber;
	private int bin;
	private int bout;
	private int totalBytes;
	private long timestamp;

	public long getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(long phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public int getBin() {
		return bin;
	}

	public void setBin(int bin) {
		this.bin = bin;
	}

	public int getBout() {
		return bout;
	}

	public void setBout(int bout) {
		this.bout = bout;
	}

	public int getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(int totalBytes) {
		this.totalBytes = totalBytes;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
