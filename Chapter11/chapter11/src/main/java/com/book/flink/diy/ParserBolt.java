package com.book.flink.diy;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ParserBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1271439619204966337L;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String valueByField = input.getString(0);
		System.out.println("field value "+ valueByField);
		String[] split = valueByField.split(",");
		PacketDetailDTO tdrPacketDetailDTO = new PacketDetailDTO();
		tdrPacketDetailDTO.setPhoneNumber(Long.parseLong(split[0]));
		tdrPacketDetailDTO.setBin(Integer.parseInt(split[1]));
		tdrPacketDetailDTO.setBout(Integer.parseInt(split[2]));
		tdrPacketDetailDTO.setTimestamp(Long.parseLong(split[3]));

		collector.emit("tdrstream", new Values(tdrPacketDetailDTO));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tdrstream", new Fields("tdrstream"));
	}

}
