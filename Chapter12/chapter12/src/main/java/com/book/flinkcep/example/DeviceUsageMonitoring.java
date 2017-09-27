package com.book.flinkcep.example;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

public class DeviceUsageMonitoring {
	private static final long TXN_TIMESPAN_SEC = 10;

	public static void main(String[] args) throws Exception {

		List<String> allTxn = new ArrayList<String>();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		properties.setProperty("auto.offset.reset", "latest");
		FlinkKafkaConsumer08<DeviceEvent> deviceEventConsumer = new FlinkKafkaConsumer08<>("device-data",
				new DeviceSchema(), properties);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<DeviceEvent> messageStream = env.addSource(deviceEventConsumer)
				.assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

		Pattern<DeviceEvent, ?> alertPattern = Pattern.<DeviceEvent>begin("first").subtype(DeviceEvent.class)
				.where(new DeviceFilterFunction()).followedBy("second").subtype(DeviceEvent.class)
				.where(new DeviceFilterFunction()).within(Time.seconds(TXN_TIMESPAN_SEC));

		PatternStream<DeviceEvent> tempPatternStream = CEP.pattern(messageStream.rebalance().keyBy("phoneNumber"),
				alertPattern);

		DataStream<DeviceAlert> alert = tempPatternStream
				.select(new PatternSelectFunction<DeviceEvent, DeviceAlert>() {
					private static final long serialVersionUID = 1L;

					@Override
					public DeviceAlert select(Map<String, DeviceEvent> pattern) {
						DeviceEvent first = (DeviceEvent) pattern.get("first");
						DeviceEvent second = (DeviceEvent) pattern.get("second");
						allTxn.clear();
						allTxn.add(first.getPhoneNumber() + " used " + ((first.getBin() + first.getBout())/1024/1024) +
								" MB at " + new Date(first.getTimestamp()));
						allTxn.add(second.getPhoneNumber() + " used " + ((second.getBin() + second.getBout())/1024/1024) +
								" MB at " + new Date(second.getTimestamp()));
						return new DeviceAlert(first.getPhoneNumber(), allTxn);
					}
				});

		alert.print();
		env.execute("CEP monitoring job");
	}
}

class DeviceFilterFunction implements FilterFunction<DeviceEvent> {

	private static final long serialVersionUID = 5540041835049961669L;
	final long TOTAL_BYTES_THRESHOLD = 15 * 1024 * 1024;

	@Override // now its dummy and always returns true
	public boolean filter(DeviceEvent value) {
		long totalBytes = value.getBin() + value.getBout();
		if (totalBytes > TOTAL_BYTES_THRESHOLD)
			return true;
		else
			return false;
	}

}