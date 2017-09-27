package com.book.simulator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.common.serialization.StringDeserializer;

import com.book.domain.Location;
import com.book.domain.VehicleAlertInfo;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * This class is used to load static information in to Hazelcast like 1. Map 1
 * contains information of vehicle and its owner. 2. Map 2 contains information
 * of geo fencing range configured for each vehicle by user
 * 
 * @author Sgupta
 *
 */
public class HazelCastLoader {
	static private Random r = new Random();
	static private ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Provide phonenumber, topic and threshold distance for each vehicle");
			System.exit(1);
		}
		
		// Phone number on which user will get alert.
		String phoneNUmber = args[0];
		
		// Topic Name from which static data will be read and feed into Hazelcast
		String topic = args[1];
		
		// Threshold distance in meters.
		int distanceFromVehicleStartPoint = Integer.parseInt(args[2]);

		// Get vehicle alert info map from Hazelcast
		Map<String, VehicleAlertInfo> vehicleAlertMap = getHCAlertInfoMap();

		// Get message from Kafka and push into Hazelcast
		getAndLoadHCMap(phoneNUmber, topic, distanceFromVehicleStartPoint, vehicleAlertMap);
	}

	private static void getAndLoadHCMap(String phoneNUmber, String topic, int distanceFromVehicleStartPoint,
			Map<String, VehicleAlertInfo> vehicleAlertMap) {
		Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "myGroup"+r.nextInt(100));
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("auto.offset.reset", "smallest");
		
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1)); // number of consumer threads

        KafkaStream<byte[], byte[]> stream = consumer.createMessageStreams(topicCountMap).get(topic).get(0);

        ConsumerIterator<byte[], byte[]> it = stream.iterator();
		
        while(it.hasNext()) {
        	try {
        		String message = new String(it.next().message());
				System.out.println("Message: "+ message);
				Map<String, Location> readValue = objectMapper.readValue(message,
						new TypeReference<Map<String, Location>>() {
						});
				for (String vehicleId : readValue.keySet()) {
					VehicleAlertInfo vehicleAlertInfo = new VehicleAlertInfo();
					vehicleAlertInfo.setVehicleId(vehicleId);
					vehicleAlertInfo.setLatitude(readValue.get(vehicleId).getLatitude());
					vehicleAlertInfo.setLongitude(readValue.get(vehicleId).getLongitude());
					vehicleAlertInfo.setDistance(distanceFromVehicleStartPoint);
					vehicleAlertInfo.setPhoneNumber(phoneNUmber);
					vehicleAlertMap.put(vehicleId, vehicleAlertInfo);
				}
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
        }
        consumer.shutdown();
	}

	private static Map<String, VehicleAlertInfo> getHCAlertInfoMap() {
		HazelcastInstance client = getHazelcastClient();
		Map<String, VehicleAlertInfo> vehicleAlertMap = client.getMap("vehicleAlertMap");
		vehicleAlertMap.clear();
		return vehicleAlertMap;
	}

	private static HazelcastInstance getHazelcastClient() {
		ClientConfig clientConfig = new ClientConfig();
		return HazelcastClient.newHazelcastClient(clientConfig);
	}
}
