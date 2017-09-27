package com.book.flink.diy;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import com.book.flinkcep.example.DeviceEvent;
import com.book.flinkcep.example.DeviceSchema;

public class FlinkESConnector {
	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		properties.setProperty("auto.offset.reset", "latest");
		FlinkKafkaConsumer08<DeviceEvent> flinkKafkaConsumer08 = new FlinkKafkaConsumer08<>("device-data",
				new DeviceSchema(), properties);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<DeviceEvent> messageStream = env.addSource(flinkKafkaConsumer08);
		
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "my-application");
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");

		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		messageStream.addSink(new ElasticsearchSink<DeviceEvent>(config, transportAddresses, new ESSink()));
		env.execute();
	}
}

class ESSink implements ElasticsearchSinkFunction<DeviceEvent> {
	private static final long serialVersionUID = -4286031843082751966L;
	
    @Override
    public void process(DeviceEvent element, RuntimeContext ctx, RequestIndexer indexer) {
    	Map<String, Object> json = new HashMap<>();
		json.put("phoneNumber", element.getPhoneNumber());
		json.put("bin", element.getBin());
		json.put("bout", element.getBout());
		json.put("timestamp", element.getTimestamp());
		
		System.out.println(json);
		
        IndexRequest source = Requests.indexRequest()
                .index("flink-test")
                .type("flink-log")
                .source(json);
        
        indexer.add(source);
    }
}