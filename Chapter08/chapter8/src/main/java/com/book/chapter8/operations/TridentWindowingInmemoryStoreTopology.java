package com.book.chapter8.operations;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.SlidingCountWindow;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Sample application of trident windowing which uses inmemory store for storing tuples in window.
 */
public class TridentWindowingInmemoryStoreTopology {

    public static StormTopology buildTopology(WindowsStoreFactory windowStore, WindowConfig windowConfig) throws Exception {
        TridentKafkaConfig config = new TridentKafkaConfig(new ZkHosts("localhost:2181"), "test");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(config);
        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("spout1", spout).each(new Fields("str"),
                new Split(), new Fields("word"))
                .window(windowConfig, windowStore, new Fields("word"), new CountAsAggregator(), new Fields("count"))
                .peek(new Consumer() {
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

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        WindowsStoreFactory mapState = new InMemoryWindowsStoreFactory();

        if (args.length == 0) {
            List<? extends WindowConfig> list = Arrays.asList(
                   // SlidingCountWindow.of(100, 10)
                   // TumblingCountWindow.of(100)
                   //SlidingDurationWindow.of(new BaseWindowedBolt.Duration(6, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS))
                   TumblingDurationWindow.of(new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS))
            );

            for (WindowConfig windowConfig : list) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("wordCounter", conf, buildTopology(mapState, windowConfig));
                Utils.sleep(60 * 1000);
                cluster.shutdown();
                System.out.println("===================================================================================");
                System.out.println("Completed Window config "+ windowConfig.getWindowStrategy().toString());
                System.out.println("===================================================================================");
            }
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(mapState, SlidingCountWindow.of(1000, 100)));
        }
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
