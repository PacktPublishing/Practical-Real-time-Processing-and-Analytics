package com.book.chapter7.diy;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class TelecomProcessorTopology {
	public static void main(String[] args) {
		Config config = new Config();
		config.setNumWorkers(3);
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		String zkConnString = "localhost:2181";
		String topicName = "storm-diy";
		
		BrokerHosts hosts = new ZkHosts(zkConnString);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName , "/" + topicName, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		topologyBuilder.setSpout("spout", kafkaSpout, 1);
		topologyBuilder.setBolt("parser", new ParseAndUsageBolt(), 1).shuffleGrouping("spout");
		topologyBuilder.setBolt("usageCassandra", new UsageCassandraBolt("localhost", "usage"), 1).shuffleGrouping("parser", "usagestream");
		topologyBuilder.setBolt("tdrCassandra", new TDRCassandraBolt("localhost", "tdr"), 1).shuffleGrouping("parser", "tdrstream");
		
		LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("storm-diy", config, topologyBuilder.createTopology());
	}
}
