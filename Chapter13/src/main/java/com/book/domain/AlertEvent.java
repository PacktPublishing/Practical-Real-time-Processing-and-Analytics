package com.book.domain;

import java.io.Serializable;

public class AlertEvent implements Serializable{
	
	private static final long serialVersionUID = -1045553752529782694L;
	private double thresholdDistance;
	private double actualDistance;
	private double startingLatitude;
	private double startingLongitude;
	private double actualLatitude;
	private double actualLongitude;
	private String vehicleId;
	private long timeStamp;
	private String phoneNumber;
	
	public double getThresholdDistance() {
		return thresholdDistance;
	}
	public void setThresholdDistance(double thresholdDistance) {
		this.thresholdDistance = thresholdDistance;
	}
	public double getActualDistance() {
		return actualDistance;
	}
	public void setActualDistance(double actualDistance) {
		this.actualDistance = actualDistance;
	}
	public String getVehicleId() {
		return vehicleId;
	}
	public void setVehicleId(String vehicleId) {
		this.vehicleId = vehicleId;
	}
	public double getStartingLatitude() {
		return startingLatitude;
	}
	public void setStartingLatitude(double startingLatitude) {
		this.startingLatitude = startingLatitude;
	}
	public double getStartingLongitude() {
		return startingLongitude;
	}
	public void setStartingLongitude(double startingLongitude) {
		this.startingLongitude = startingLongitude;
	}
	public double getActualLatitude() {
		return actualLatitude;
	}
	public void setActualLatitude(double actualLatitude) {
		this.actualLatitude = actualLatitude;
	}
	public double getActualLongitude() {
		return actualLongitude;
	}
	public void setActualLongitude(double actualLongitude) {
		this.actualLongitude = actualLongitude;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getPhoneNumber() {
		return phoneNumber;
	}
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}
	@Override
	public String toString() {
		return "AlertEvent [thresholdDistance=" + thresholdDistance + ", actualDistance=" + actualDistance
				+ ", startingLatitude=" + startingLatitude + ", startingLongitude=" + startingLongitude
				+ ", actualLatitude=" + actualLatitude + ", actualLongitude=" + actualLongitude + ", vehicleId="
				+ vehicleId + ", timeStamp=" + timeStamp + ", phoneNumber=" + phoneNumber + "]";
	}

}
