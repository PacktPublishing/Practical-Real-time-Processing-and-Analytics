package com.book.chapter8.basic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class BasicTridentTopology {

	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
				new Values("this is simple example of trident topology"), new Values(
						"this example count same words"));
		spout.setCycle(true);

		//TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(new TridentKafkaConfig(new ZkHosts("localhost:9091"), "test"));
		TridentTopology topology = new TridentTopology();
		MemoryMapState.Factory stateFactory = new MemoryMapState.Factory();
		topology
				.newStream("spout1", spout)
				.each(new Fields("sentence"), new Split(), new Fields("word"))
				.groupBy(new Fields("word")).persistentAggregate(stateFactory, new Count(),
						new Fields("count")).newValuesStream()
				.filter(new DisplayOutputFilter()).parallelismHint(6);
		
		Config config = new Config();
		config.setNumWorkers(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-trident-example", config, topology.build());
	}
}

class Split extends BaseFunction {
	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = tuple.getString(0);
		for (String word : sentence.split(" ")) {
			collector.emit(new Values(word));
		}
	}
}