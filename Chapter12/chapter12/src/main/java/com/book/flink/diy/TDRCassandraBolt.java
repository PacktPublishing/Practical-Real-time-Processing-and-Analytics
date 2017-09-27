package com.book.flink.diy;

import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TDRCassandraBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private Cluster cluster;
	private Session session;
	private String hostname;
	private String keyspace;

	public TDRCassandraBolt(String hostname, String keyspace) {
		this.hostname = hostname;
		this.keyspace = keyspace;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		cluster = Cluster.builder().addContactPoint(hostname).build();
		session = cluster.connect(keyspace);
	}

	public void execute(Tuple input, BasicOutputCollector arg1) {
		
		PacketDetailDTO packetDetailDTO = (PacketDetailDTO) input.getValueByField("tdrstream");
		System.out.println("field value "+ packetDetailDTO);
		session.execute("INSERT INTO packet_tdr (phone_number, bin, bout, timestamp) VALUES ("
				+ packetDetailDTO.getPhoneNumber()
				+ ", "
				+ packetDetailDTO.getBin()
				+ ","
				+ packetDetailDTO.getBout()
				+ "," + packetDetailDTO.getTimestamp() + ")");
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

	@Override
	public void cleanup() {
		session.close();
		cluster.close();
	}
}
