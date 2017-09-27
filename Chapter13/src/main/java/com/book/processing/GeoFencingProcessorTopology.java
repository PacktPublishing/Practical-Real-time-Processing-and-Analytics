package com.book.processing;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * This is complete topology to bind spout and all bolts.
 * 
 * @author SGupta
 *
 */
public class GeoFencingProcessorTopology {
	public static void main(String[] args) {
		
		if(args.length <1){
			System.out.println("Please mention deployment mode either local or cluster");
			System.exit(1);
		}
		
		String deploymentMode = args[0];
		
		Config config = new Config();
		config.setNumWorkers(3);
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		String zkConnString = "localhost:2181";
		String topicName = "vehicle-data";
		String hcHostName = "localhost";
		String hcPort = "5701";
		String esClusterName = "cluster.name";
		String esApplicationName = "my-application";
		String esHostName = "localhost";
		int esPort = 9300;
		
		BrokerHosts hosts = new ZkHosts(zkConnString);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName , "/" + topicName, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		topologyBuilder.setSpout("spout", kafkaSpout, 1);
		topologyBuilder.setBolt("parser", new ParseBolt(), 1).shuffleGrouping("spout");
		topologyBuilder.setBolt("checkAndAlert", new CheckDistanceAndAlertBolt(hcHostName, hcPort), 1).shuffleGrouping("parser","parsedstream");
		topologyBuilder.setBolt("saveTDR", new ElasticSearchBolt("vehicle-tdr", "tdr",esClusterName, esApplicationName,esHostName, esPort),1).shuffleGrouping("parser","parsedstream");
		topologyBuilder.setBolt("generateAlert", new GenerateAlertBolt(hcHostName, hcPort), 1).shuffleGrouping("checkAndAlert", "alertInfo");
		topologyBuilder.setBolt("saveAlert", new ElasticSearchBolt("vehicle-alert", "alert",esClusterName, esApplicationName,esHostName, esPort), 1).shuffleGrouping("generateAlert", "generatedAlertInfo");
		
		LocalCluster cluster = new LocalCluster();
		if (deploymentMode.equalsIgnoreCase("local")) {
			System.out.println("Submitting topology on local");
			cluster.submitTopology(topicName, config, topologyBuilder.createTopology());
		} else {
	        try {
	        	System.out.println("Submitting topology on cluster");
				StormSubmitter.submitTopology(topicName, config, topologyBuilder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				e.printStackTrace();
			}
		}
	}
}
