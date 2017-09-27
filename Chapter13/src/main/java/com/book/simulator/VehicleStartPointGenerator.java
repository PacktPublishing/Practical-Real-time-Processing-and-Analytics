package com.book.simulator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.book.domain.Location;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is used to generate vehicle start point for number of vehicle
 * specified by user.
 * 
 * @author SGupta
 *
 */
public class VehicleStartPointGenerator {
	static private ObjectMapper objectMapper = new ObjectMapper();
	static private Random r = new Random();
	static private String BROKER_1_CONNECTION_STRING = "localhost:9092";
	static private String KAFKA_TOPIC_STATIC_DATA = "vehicle-static-data";

	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("Provide number of vehicle");
			System.exit(1);
		}

		// Number of vehicles for which data needs to be generated.
		int numberOfvehicle = Integer.parseInt(args[0]);

		// Get producer to push data into Kafka
		KafkaProducer<Integer, String> producer = configureKafka();

		// Get vehicle start point.
		Map<String, Location> vehicleStartPoint = getVehicleStartPoints(numberOfvehicle);
		
		// Push data into Kafka
		pushVehicleStartPointToKafka(vehicleStartPoint, producer);

		producer.close();
	}

	private static KafkaProducer<Integer, String> configureKafka() {
		Properties properties = new Properties();

		properties.put("bootstrap.servers", BROKER_1_CONNECTION_STRING);
		properties.put("key.serializer", StringSerializer.class.getName());
		properties.put("value.serializer", StringSerializer.class.getName());
		properties.put("auto.offset.reset", "smallest");
		properties.put("acks", "1");

		KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
		return producer;
	}

	private static Map<String, Location> getVehicleStartPoints(int numberOfvehicle) {
		Map<String, Location> vehicleStartPoint = new HashMap<String, Location>();
		for (int i = 1; i <= numberOfvehicle; i++) {
			vehicleStartPoint.put("v" + i,
					new Location((r.nextDouble() * -180.0) + 90.0, (r.nextDouble() * -360.0) + 180.0));
		}
		System.out.println(vehicleStartPoint);
		return vehicleStartPoint;
	}

	private static void pushVehicleStartPointToKafka(Map<String, Location> vehicleStartPoint,
			KafkaProducer<Integer, String> producer) {
		ProducerRecord<Integer, String> data = null;
		try {
			data = new ProducerRecord<Integer, String>(KAFKA_TOPIC_STATIC_DATA,
					objectMapper.writeValueAsString(vehicleStartPoint));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		producer.send(data);
	}
}
