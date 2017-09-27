package com.book.processing;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.book.domain.AlertEvent;
import com.book.domain.VehicleSensor;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This bolt is used to create index in Elasticsearch.
 * 
 * @author SGupta
 *
 */
public class ElasticSearchBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -9123903091990273369L;
	Client client;
	PreBuiltTransportClient preBuiltTransportClient;
	ObjectMapper mapper;
	String index;
	String type;
	String clusterName;
	String applicationName;
	String host;
	int port;

	public ElasticSearchBolt(String index, String type, String clusterName, String applicationName, String host,
			int port) {
		this.index = index;
		this.type = type;
		this.clusterName = clusterName;
		this.applicationName = applicationName;
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		mapper = new ObjectMapper(); 
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		preBuiltTransportClient = new PreBuiltTransportClient(settings);
		client = preBuiltTransportClient
				.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)));
	}

	@Override
	public void cleanup() {
		preBuiltTransportClient.close();
		client.close();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No further processing is required so no emit from this bolt.
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String, Object> value = null;

		// Check type and based on it processing the value
		if (type.equalsIgnoreCase("tdr")) {
			VehicleSensor vehicleSensor = (VehicleSensor) input.getValueByField("parsedstream");
			// Converting POJO object into Map
			value = convertVehicleSensortoMap(vehicleSensor);
		} else if (type.equalsIgnoreCase("alert")) {
			AlertEvent alertEvent = (AlertEvent) input.getValueByField("generatedAlertInfo");
			//Converting POJO object into Map
			value = convertVehicleAlerttoMap(alertEvent);
		}

		// Inserting into Elasticsearch
		IndexResponse response = client.prepareIndex(index, type).setSource(value).get();
		System.out.println(response.status());
	}

	public Map<String, Object> convertVehicleSensortoMap(VehicleSensor vehicleSensor) {
		System.out.println("Orignal value  " + vehicleSensor);
		Map<String, Object> convertedValue = new HashMap<String, Object>();
		Map<String, Object> coords = new HashMap<>();
		
		convertedValue.put("vehicle_id", vehicleSensor.getVehicleId());
		coords.put("lat", vehicleSensor.getLatitude());
		coords.put("lon", vehicleSensor.getLongitude());
		convertedValue.put("coords", coords);
		convertedValue.put("speed", vehicleSensor.getSpeed());
		convertedValue.put("timestamp", new Date(vehicleSensor.getTimeStamp()));

		System.out.println("Converted value  " + convertedValue);
		return convertedValue;
	}

	public Map<String, Object> convertVehicleAlerttoMap(AlertEvent alertEvent) {
		System.out.println("Orignal value  " + alertEvent);
		Map<String, Object> convertedValue = new HashMap<String, Object>();
		Map<String, Object> expected_coords = new HashMap<>();
		Map<String, Object> actual_coords = new HashMap<>();

		convertedValue.put("vehicle_id", alertEvent.getVehicleId());
		expected_coords.put("lat", alertEvent.getStartingLatitude());
		expected_coords.put("lon", alertEvent.getStartingLongitude());
		convertedValue.put("expected_coords", expected_coords);
		actual_coords.put("lat", alertEvent.getActualLatitude());
		actual_coords.put("lon", alertEvent.getActualLongitude());
		convertedValue.put("actual_coords", actual_coords);
		convertedValue.put("expected_distance", alertEvent.getThresholdDistance());
		convertedValue.put("actual_distance", alertEvent.getActualDistance());
		convertedValue.put("timestamp", new Date(alertEvent.getTimeStamp()));

		System.out.println("Converted value  " + convertedValue);
		return convertedValue;
	}
}
