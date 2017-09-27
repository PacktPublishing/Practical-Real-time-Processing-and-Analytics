package com.book.processing;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.book.domain.VehicleSensor;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This bolt is used to parse Vehicle real time data from Kafka and convert it
 * into {@link VehicleSensor} POJO object
 * 
 * @author SGupta
 *
 */
public class ParseBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -2557041273635037199L;
	ObjectMapper objectMapper;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		objectMapper = new ObjectMapper();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// Take default message from KafkaSpout
		String valueByField = input.getString(0);
		VehicleSensor vehicleSensor = null;
		try {
			//Covert JSON value into VehicleSensor object
			vehicleSensor = objectMapper.readValue(valueByField, VehicleSensor.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Emit tuple to next bolt with steamId as parsedstream
		collector.emit("parsedstream", new Values(vehicleSensor));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Declare Stream with streamId as parsedstream with fields parsedstream
		declarer.declareStream("parsedstream", new Fields("parsedstream"));
	}
}
