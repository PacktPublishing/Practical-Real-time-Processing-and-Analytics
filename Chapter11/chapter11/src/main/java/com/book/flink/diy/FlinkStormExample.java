package com.book.flink.diy;

import org.apache.flink.storm.api.FlinkTopology;

import backtype.storm.topology.TopologyBuilder;

public class FlinkStormExample {
	public static void main(String[] args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("spout", new FileSpout("/tmp/device-data.txt"), 1);
		topologyBuilder.setBolt("parser", new ParserBolt(), 1).shuffleGrouping("spout");
		topologyBuilder.setBolt("tdrCassandra", new TDRCassandraBolt("localhost", "tdr"), 1).shuffleGrouping("parser", "tdrstream");
		
		FlinkTopology.createTopology(topologyBuilder).execute();
	}
}
