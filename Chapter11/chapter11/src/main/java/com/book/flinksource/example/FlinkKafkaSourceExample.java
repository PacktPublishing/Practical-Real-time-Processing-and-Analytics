package com.book.flinksource.example;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class FlinkKafkaSourceExample {

	public static void main(String args[]) throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		properties.setProperty("auto.offset.reset", "latest");  
		FlinkKafkaConsumer08<String> flinkKafkaConsumer08 = new FlinkKafkaConsumer08<>("flink-test",
				new SimpleStringSchema(), properties);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> messageStream = env.addSource(flinkKafkaConsumer08);

		// print() will write the contents of the stream to the TaskManager's
		// standard out stream
		// the rebelance call is causing a repartitioning of the data so that
		// all machines
		// see the messages (for example in cases when "num kafka partitions" <
		// "num flink operators"
		messageStream.rebalance().map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public String map(String value) throws Exception {
				return "Kafka and Flink says: " + value;
			}
		}).print();

		env.execute();
	}

}
