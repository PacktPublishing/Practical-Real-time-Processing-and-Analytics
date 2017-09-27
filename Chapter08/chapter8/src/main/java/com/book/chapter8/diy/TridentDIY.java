package com.book.chapter8.diy;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentDIY {
	public static void main(String args[]) {
		TridentKafkaConfig config = new TridentKafkaConfig(new ZkHosts(
				"localhost:2181"), "storm-trident-diy");
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(
				config);
		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout).filter(new ExcludePhoneNumber())
				.each(new Fields("str"), new DeviceInfoExtractor(), new Fields("phone", "bytes"))
				.groupBy(new Fields("phone"))
				.aggregate(new Fields("bytes", "phone"), new Sum(), new Fields("sum"))
				.applyAssembly(new FirstN(10, "sum"))
				.each(new Fields("phone", "sum"), new Debug());
		
		Config config1 = new Config();
		config1.setNumWorkers(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-trident-diy", config1, topology.build());
	}
}

class ExcludePhoneNumber extends BaseFilter {
	private static final long serialVersionUID = 7961541061613235361L;

	public boolean isKeep(TridentTuple tuple) {
		return !tuple.get(0).toString().contains("9999999950");
    }
}

class DeviceInfoExtractor extends BaseFunction {

	private static final long serialVersionUID = 488985511293326495L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String event = tuple.getString(0);
		System.out.println(event);
		String[] splittedEvent = event.split(",");
		if(splittedEvent.length>1){
			long phoneNumber = Long.parseLong(splittedEvent[0]);
			int bin = Integer.parseInt(splittedEvent[1]);
			int bout = Integer.parseInt(splittedEvent[2]);
			
			int totalBytesTransferred = bin + bout;
			System.out.println(phoneNumber+":"+bin+":"+bout);
			collector.emit(new Values(phoneNumber, totalBytesTransferred));
		}
	}
}