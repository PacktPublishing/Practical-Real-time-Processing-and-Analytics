package com.book.flinkcep.example;

public class DeviceEvent {

	private long phoneNumber;
	private int bin;
	private int bout;
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

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public static DeviceEvent fromString(String line) {

		String[] tokens = line.split(",");

		if (tokens.length != 4) {
			throw new RuntimeException("Invalid record: " + line);
		}

		DeviceEvent deviceEvent = new DeviceEvent();

		deviceEvent.phoneNumber = Long.parseLong(tokens[0]);
		deviceEvent.bin = Integer.parseInt(tokens[1]);
		deviceEvent.bout = Integer.parseInt(tokens[2]);
		deviceEvent.timestamp = Long.parseLong(tokens[3]);

		return deviceEvent;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + bin;
		result = prime * result + bout;
		result = prime * result + (int) (phoneNumber ^ (phoneNumber >>> 32));
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DeviceEvent other = (DeviceEvent) obj;
		if (bin != other.bin)
			return false;
		if (bout != other.bout)
			return false;
		if (phoneNumber != other.phoneNumber)
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DeviceEvent [phoneNumber=" + phoneNumber + ", bin=" + bin + ", bout=" + bout + ", timestamp="
				+ timestamp + "]";
	}
}
