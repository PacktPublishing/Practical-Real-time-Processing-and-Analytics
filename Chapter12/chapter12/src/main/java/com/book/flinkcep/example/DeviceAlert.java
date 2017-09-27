package com.book.flinkcep.example;

import java.util.List;

public class DeviceAlert {

	private long phoneNumber;
	private List<String> message;

	public long getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(long phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public List<String> getMessage() {
		return message;
	}

	public void setMessage(List<String> message) {
		this.message = message;
	}

	public DeviceAlert(long phoneNumber, List<String> message) {
		super();
		this.phoneNumber = phoneNumber;
		this.message = message;
	}

	@Override
	public String toString() {
		return "DeviceAlert [phoneNumber=" + phoneNumber + ", message=" + message + "]";
	}

}