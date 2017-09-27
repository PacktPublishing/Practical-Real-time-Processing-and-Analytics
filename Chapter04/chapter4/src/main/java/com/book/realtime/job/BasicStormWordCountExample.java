package com.book.realtime.job;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class BasicStormWordCountExample {
	public static void main(String[] args) throws Exception {

		String mode = "";

		if (args.length > 0) {
			mode = args[0];
		}

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sentence-spout", new FixedSentenceSpout());
		// SentenceSpout --> SplitSentenceBolt
		builder.setBolt("split-bolt", new SplitSentenceBolt()).shuffleGrouping("sentence-spout");
		// SplitSentenceBolt --> WordCountBolt
		builder.setBolt("count-bolt", new WordCountBolt()).fieldsGrouping("split-bolt", new Fields("word"));
		// WordCountBolt --> DisplayBolt
		builder.setBolt("display-bolt", new DisplayBolt()).globalGrouping("count-bolt");
		Config config = new Config();

		if (mode.equals("cluster")) {
			System.out.println("submitting on cluster mode");
			StormSubmitter.submitTopology("word-count-topology", config, builder.createTopology());
		} else {
			System.out.println("submitting on local mode");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count-topology", config, builder.createTopology());

			Thread.sleep(20000);

			cluster.killTopology("word-count-topology");
			cluster.shutdown();
		}
	}
}
