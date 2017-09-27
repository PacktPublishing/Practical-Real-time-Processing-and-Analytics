package com.book.flink.diy;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataGenerator {
	public static void main(String args[]) {
		Properties properties = new Properties();
		 
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("acks", "1");
		 
		KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
		int counter =0;
		int nbrOfEventsRequired = Integer.parseInt(args[0]);
		while (counter<nbrOfEventsRequired) {
			StringBuffer stream = new StringBuffer();
			
			long phoneNumber = ThreadLocalRandom.current().nextLong(9999999950l,
					9999999999l);
			int bin = ThreadLocalRandom.current().nextInt(100000, 9999999);
			int bout = ThreadLocalRandom.current().nextInt(100000, 9999999);
			
			stream.append(phoneNumber);
			stream.append(",");
			stream.append(bin);
			stream.append(",");
			stream.append(bout);
			stream.append(",");
			stream.append(System.currentTimeMillis());

			System.out.println(stream.toString());
			ProducerRecord<Integer, String> data = new ProducerRecord<Integer, String>(
					"device-data", stream.toString());
			producer.send(data);
			counter++;
		}
		
		producer.close();
	}
}
