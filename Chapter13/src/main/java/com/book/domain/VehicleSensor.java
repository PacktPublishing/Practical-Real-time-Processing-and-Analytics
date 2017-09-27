package com.book.domain;

import java.io.Serializable;

public class VehicleSensor implements Serializable{

	private static final long serialVersionUID = -8242555041932122948L;
	private String vehicleId;
	private double latitude;
	private double longitude;
	private int speed;
	private long timeStamp;
	
	public VehicleSensor(){}

	public VehicleSensor(String vehicleId, double latitude, double longitude, int speed, long timeStamp) {
		super();
		this.vehicleId = vehicleId;
		this.latitude = latitude;
		this.longitude = longitude;
		this.speed = speed;
		this.timeStamp = timeStamp;
	}

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

	public int getSpeed() {
		return speed;
	}

	public void setSpeed(int speed) {
		this.speed = speed;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	@Override
	public String toString() {
		return "VehicleSensor [vehicleId=" + vehicleId + ", latitude=" + latitude + ", longitude=" + longitude
				+ ", speed=" + speed + ", timeStamp=" + timeStamp + "]";
	}
}
