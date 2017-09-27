package com.book.simulator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.book.domain.Location;
import com.book.domain.VehicleSensor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * This class is used to generate real time vehicle data with updated location
 * within distance in radius of user specified value. Messages are pushed into Kafka topic.
 * 
 * @author SGupta
 *
 */
public class VehicleDataGeneration {

	static private ObjectMapper objectMapper = new ObjectMapper();
	static private Random r = new Random();
	static private String BROKER_1_CONNECTION_STRING = "localhost:9092";
	static private String ZOOKEEPER_CONNECTION_STRING = "localhost:2181";
	static private String KAFKA_TOPIC_STATIC_DATA = "vehicle-static-data";
	static private String KAFKA_TOPIC_REAL_TIME_DATA = "vehicle-data";

	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Provide total number of records and range of distance from start point");
			System.exit(1);
		}

		//Total number of records this simulator will generate
		int totalNumberOfRecords = Integer.parseInt(args[0]);
		
		//Distance in meters as Radius
		int distanceFromVehicleStartPoint = Integer.parseInt(args[1]);

		// Get Kafka producer
		KafkaProducer<Integer, String> producer = configureKafka();

		// Get Vehicle Start Points
		Map<String, Location> vehicleStartPoint = getVehicleStartPoints();
		
		// Generate data within distance and push to Kafka
		generateDataAndPushToKafka(producer, vehicleStartPoint.size(), totalNumberOfRecords,
				distanceFromVehicleStartPoint, vehicleStartPoint);
		
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

	private static void generateDataAndPushToKafka(KafkaProducer<Integer, String> producer, int numberOfvehicle,
			int totalNumberOfRecords, int distanceFromVehicleStartPoint, Map<String, Location> vehicleStartPoint) {
		for (int i = 0; i < totalNumberOfRecords; i++) {
			int vehicleNumber = r.nextInt(numberOfvehicle);
			String vehicleId = "v" + (vehicleNumber + 1);
			Location currentLocation = vehicleStartPoint.get(vehicleId);
			Location locationInLatLngRad = getLocationInLatLngRad(distanceFromVehicleStartPoint, currentLocation);
			System.out.println(
					"Vehicle number is " + vehicleId + " and location is " + locationInLatLngRad + " with distance of "
							+ (getDistanceFromLatLonInKm(currentLocation.getLatitude(), currentLocation.getLongitude(),
									locationInLatLngRad.getLatitude(), locationInLatLngRad.getLongitude()) * 1000));

			VehicleSensor vehicleSensor = new VehicleSensor(vehicleId, locationInLatLngRad.getLatitude(),
					locationInLatLngRad.getLongitude(), r.nextInt(100), System.currentTimeMillis());

			ProducerRecord<Integer, String> data = null;
			try {
				data = new ProducerRecord<Integer, String>(KAFKA_TOPIC_REAL_TIME_DATA,
						objectMapper.writeValueAsString(vehicleSensor));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			producer.send(data);
		}
	}

	private static Map<String, Location> getVehicleStartPoints() {
		Map<String, Location> vehicleStartPoint = new HashMap<String, Location>();
		Properties props = new Properties();
		props.put("zookeeper.connect", ZOOKEEPER_CONNECTION_STRING);
		props.put("group.id", "DataLoader" + r.nextInt(100));
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("auto.offset.reset", "smallest");

		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(KAFKA_TOPIC_STATIC_DATA, new Integer(1)); 

		KafkaStream<byte[], byte[]> stream = consumer.createMessageStreams(topicCountMap).get(KAFKA_TOPIC_STATIC_DATA)
				.get(0);

		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			String message = new String(it.next().message());
			try {
				vehicleStartPoint = objectMapper.readValue(message, new TypeReference<Map<String, Location>>() {
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		}
		consumer.shutdown();
		return vehicleStartPoint;
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

	protected static Location getLocationInLatLngRad(double radiusInMeters, Location currentLocation) {
		double x0 = currentLocation.getLongitude();
		double y0 = currentLocation.getLatitude();

		Random random = new Random();

		// Convert radius from meters to degrees.
		double radiusInDegrees = radiusInMeters / 111320f;

		// Get a random distance and a random angle.
		double u = random.nextDouble();
		double v = random.nextDouble();
		double w = radiusInDegrees * Math.sqrt(u);
		double t = 2 * Math.PI * v;
		// Get the x and y delta values.
		double x = w * Math.cos(t);
		double y = w * Math.sin(t);

		// Compensate the x value.
		double new_x = x / Math.cos(Math.toRadians(y0));

		double foundLatitude;
		double foundLongitude;

		foundLatitude = y0 + y;
		foundLongitude = x0 + new_x;

		Location copy = new Location(currentLocation);
		copy.setLatitude(foundLatitude);
		copy.setLongitude(foundLongitude);
		return copy;
	}
}
