package com.book.chapter8.operations;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentOperations {
	public static void main(String args[]) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"), 3,
				new Values(1, 2), new Values(3, 4),
				new Values(7, 3));
		spout.setCycle(false);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout1", spout)
				.each(new Fields("a", "b"), new PerformDiffFunction(),
						new Fields("d")).peek(new Consumer() {
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

class MyFilter extends BaseFilter {
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getInteger(0) == 1 && tuple.getInteger(1) == 2;
    }
}

class PerformDiffFunction extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		int number1 = tuple.getInteger(0);
		int number2 = tuple.getInteger(1);

		if (number2 > number1) {
			collector.emit(new Values(number2 - number1));
		}
	}

}
