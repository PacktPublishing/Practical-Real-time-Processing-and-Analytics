package com.book.processing;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.book.domain.AlertEvent;
import com.book.domain.VehicleAlertInfo;
import com.book.domain.VehicleSensor;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

/**
 * This bolt is used to first check distance between start point of vehicle and
 * current location of vehicle. Only those tuples are emitted which are related
 * to alerts.
 * 
 * @author SGupta
 *
 */
public class CheckDistanceAndAlertBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -8873075873347212209L;

	private Map<String, VehicleAlertInfo> vehicleAlertMap;
	private HazelcastInstance hazelcastClient;
	private String host;
	private String port;

	public CheckDistanceAndAlertBolt(String host, String port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getNetworkConfig().addAddress(host + ":" + port);
		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
		vehicleAlertMap = hazelcastClient.getMap("vehicleAlertMap");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// Read input with field name as parsedstream
		VehicleSensor vehicleSensor = (VehicleSensor) input.getValueByField("parsedstream");

		String vehicleId = vehicleSensor.getVehicleId();

		// Get vehicle alert information from Hazelcast
		VehicleAlertInfo vehicleAlertInfo = vehicleAlertMap.get(vehicleId);

		// Get the distance between starting location and current location.
		double actualDistance = getDistanceFromLatLonInKm(vehicleAlertInfo.getLatitude(),
				vehicleAlertInfo.getLongitude(), vehicleSensor.getLatitude(), vehicleSensor.getLongitude()) * 1000;
		long thresholdDistance = vehicleAlertInfo.getDistance();
		// If current distance is more than threshold distance then emit tuple
		// to next bolt
		if (actualDistance > thresholdDistance) {
			AlertEvent alertEvent = new AlertEvent();
			alertEvent.setActualDistance(actualDistance);
			alertEvent.setThresholdDistance(thresholdDistance);
			alertEvent.setStartingLatitude(vehicleAlertInfo.getLatitude());
			alertEvent.setStartingLongitude(vehicleAlertInfo.getLongitude());
			alertEvent.setActualLatitude(vehicleSensor.getLatitude());
			alertEvent.setActualLongitude(vehicleSensor.getLongitude());
			alertEvent.setVehicleId(vehicleId);
			alertEvent.setTimeStamp(System.currentTimeMillis());
			alertEvent.setPhoneNumber(vehicleAlertInfo.getPhoneNumber());

			collector.emit("alertInfo", new Values(alertEvent));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("alertInfo", new Fields("alertInfo"));
	}

	@Override
	public void cleanup() {
		hazelcastClient.shutdown();
	}

	public static double getDistanceFromLatLonInKm(double lat1, double lon1, double lat2, double lon2) {
		int R = 6371; // Radius of the earth in km
		double dLat = deg2rad(lat2 - lat1); // deg2rad below
		double dLon = deg2rad(lon2 - lon1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double d = R * c; // Distance in km
		return d;
	}

	private static double deg2rad(double deg) {
		return deg * (Math.PI / 180);
	}
}
