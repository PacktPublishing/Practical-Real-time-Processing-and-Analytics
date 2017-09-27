package com.book.chapter7.visualization.example;

import java.io.IOException;
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -9123903091990273369L;
	Client client;
	PreBuiltTransportClient preBuiltTransportClient;
	ObjectMapper mapper;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		// instance a json mapper
		mapper = new ObjectMapper(); // create once, reuse
		Settings settings = Settings.builder()
				.put("cluster.name", "my-application").build();
		preBuiltTransportClient = new PreBuiltTransportClient(settings);
		client = preBuiltTransportClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)));
	}
	
	@Override
	public void cleanup() {
		preBuiltTransportClient.close();
		client.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String valueByField = input.getString(0);
		System.out.println(valueByField);
		try {
			IndexResponse response = client.prepareIndex("pub-nub", "sensor-data")
					.setSource(convertStringtoMap(valueByField)).get();
			System.out.println(response.status());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Map<String,Object> convertStringtoMap(String fieldValue) throws JsonParseException, JsonMappingException, IOException {
		System.out.println("Orignal value  "+ fieldValue);
		Map<String,Object> convertedValue = new HashMap<>();
		Map<String,Object> readValue = mapper.readValue(fieldValue, new TypeReference<Map<String,Object>>() {});

		convertedValue.put("ambient_temperature", Double.parseDouble(String.valueOf(readValue.get("ambient_temperature"))));
		convertedValue.put("photosensor", Double.parseDouble(String.valueOf(readValue.get("photosensor"))));
		convertedValue.put("humidity", Double.parseDouble(String.valueOf(readValue.get("humidity"))));
		convertedValue.put("radiation_level", Integer.parseInt(String.valueOf(readValue.get("radiation_level"))));
		convertedValue.put("sensor_uuid", readValue.get("sensor_uuid"));
		convertedValue.put("timestamp", new Date());
		
		System.out.println("Converted value  "+ convertedValue);
		return convertedValue;
	}
}
