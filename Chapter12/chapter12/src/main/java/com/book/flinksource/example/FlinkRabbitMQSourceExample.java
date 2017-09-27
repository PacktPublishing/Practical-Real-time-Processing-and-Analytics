package com.book.flinksource.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class FlinkRabbitMQSourceExample {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
		
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			    .setHost("localhost")
			    .setPort(5672)
			    .setVirtualHost("/")
			    .setUserName("guest")
			    .setPassword("guest")
			    .build();
		
		final DataStream<String> stream = env
			    .addSource(new RMQSource<String>(
			        connectionConfig,            // config for the RabbitMQ connection
			        "flink-test",                 // name of the RabbitMQ queue to consume
			        true,                        // use correlation ids; can be false if only at-least-once is required
			        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
			    .setParallelism(1);              // non-parallel source is only required for exactly-once
		
		stream.rebalance().map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public String map(String value) throws Exception {
				return "RabbitMQ and Flink says: " + value;
			}
		}).print();

		env.execute();
	}
}
