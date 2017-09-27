package com.book.chapter7.diy;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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
		session.execute("INSERT INTO packet_tdr (phone_number, bin, bout, timestamp) VALUES ("
				+ packetDetailDTO.getPhoneNumber()
				+ ", "
				+ packetDetailDTO.getBin()
				+ ","
				+ packetDetailDTO.getBout()
				+ ",'" + packetDetailDTO.getTimestamp() + "')");
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

	@Override
	public void cleanup() {
		session.close();
		cluster.close();
	}
}
