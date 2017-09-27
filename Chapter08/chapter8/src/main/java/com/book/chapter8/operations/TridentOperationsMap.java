package com.book.chapter8.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentOperationsMap {
	public static void main(String args[]) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
				new Values("this is simple example of trident topology"),
				new Values("this example count same words"));
		spout.setCycle(false);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout1", spout).flatMap(new SplitMapFunction())
				.map(new UpperCase()).peek(new Consumer() {
					@Override
					public void accept(TridentTuple tuple) {
						System.out.print("[");
						for (int index = 0; index < tuple.size(); index++) {
							System.out.print(tuple.get(index));
							if (index < (tuple.size() - 1))
								System.out.print(",");
						}
						System.out.println("]");
					}
				});

		Config config = new Config();
		config.setNumWorkers(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-trident-example", config,
				topology.build());
	}
}

class UpperCase implements MapFunction {
	@Override
	public Values execute(TridentTuple input) {
		return new Values(input.getString(0).toUpperCase());
	}
}

class SplitMapFunction implements FlatMapFunction {
	@Override
	public Iterable<Values> execute(TridentTuple input) {
		List<Values> valuesList = new ArrayList<>();
		for (String word : input.getString(0).split(" ")) {
			valuesList.add(new Values(word));
		}
		return valuesList;
	}
}
