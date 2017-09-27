package com.book.flinksink.example;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.datastax.driver.core.Cluster;

public class FlinkCassandraConnector {
	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		properties.setProperty("auto.offset.reset", "latest");
		FlinkKafkaConsumer08<String> flinkKafkaConsumer08 = new FlinkKafkaConsumer08<>("device-data",
				new SimpleStringSchema(), properties);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple4<Long,Integer,Integer,Long>> messageStream = env.addSource(flinkKafkaConsumer08).map(new MapFunction<String, Tuple4<Long,Integer,Integer,Long>>() {

			private static final long serialVersionUID = 4723214570372887208L;

			@Override
			public Tuple4<Long,Integer,Integer,Long> map(String input) throws Exception {
				String[] inputSplits = input.split(",");
				return Tuple4.of(Long.parseLong(inputSplits[0]), Integer.parseInt(inputSplits[1]), Integer.parseInt(inputSplits[2]), Long.parseLong(inputSplits[3]));
			}
		});

		CassandraSink.addSink(messageStream).setQuery("INSERT INTO tdr.packet_tdr (phone_number, bin, bout, timestamp) values (?, ?, ? ,?);")
				.setClusterBuilder(new ClusterBuilder() {
					private static final long serialVersionUID = 1L;

					@Override
					public Cluster buildCluster(Cluster.Builder builder) {
						return builder.addContactPoint("127.0.0.1").build();
					}
				}).build();
		env.execute();
	}
}

