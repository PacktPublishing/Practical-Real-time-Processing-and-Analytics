package com.book.domain;

import java.io.Serializable;

public class Location implements Serializable {
	
	private static final long serialVersionUID = 121871205583852945L;
	private double latitude;
	private double longitude;
	
	public Location(){}

	public Location(double latitude, double longitude) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public Location(Location location) {
		latitude = location.getLatitude();
		longitude = location.getLongitude();
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

	@Override
	public String toString() {
		return "Location [latitude=" + latitude + ", longitude=" + longitude + "]";
	}

}