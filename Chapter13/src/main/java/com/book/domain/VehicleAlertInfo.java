package com.book.domain;

import java.io.Serializable;

public class VehicleAlertInfo implements Serializable{

	private static final long serialVersionUID = -1823573168903668338L;
	private String vehicleId;
	private double latitude;
	private double longitude;
	private long distance;
	private String phoneNumber;
	
	public String getVehicleId() {
		return vehicleId;
	}
	public void setVehicleId(String vehicleId) {
		this.vehicleId = vehicleId;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public long getDistance() {
		return distance;
	}
	public void setDistance(long distance) {
		this.distance = distance;
	}
	public String getPhoneNumber() {
		return phoneNumber;
	}
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}
}
