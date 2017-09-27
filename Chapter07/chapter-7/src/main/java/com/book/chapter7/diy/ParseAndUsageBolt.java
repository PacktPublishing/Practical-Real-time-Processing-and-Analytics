package com.book.chapter7.diy;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class ParseAndUsageBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1271439619204966337L;
	HazelcastInstance client;
	IMap<String, PacketDetailDTO> usageMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
		client = HazelcastClient.newHazelcastClient(clientConfig);
		usageMap = client.getMap("usage");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		PacketDetailDTO packetDetailDTO = new PacketDetailDTO();
		String valueByField = input.getString(0);
		String[] split = valueByField.split(",");
		long phoneNumber = Long.parseLong(split[0]);
		PacketDetailDTO packetDetailDTOFromMap = usageMap.get(phoneNumber);
		if (null == packetDetailDTOFromMap) {
			packetDetailDTOFromMap = new PacketDetailDTO();
		}
		packetDetailDTO.setPhoneNumber(phoneNumber);
		int bin = Integer.parseInt(split[1]);
		packetDetailDTO.setBin((packetDetailDTOFromMap.getBin() + bin));
		int bout = Integer.parseInt(split[2]);
		packetDetailDTO.setBout(packetDetailDTOFromMap.getBout() + bout);
		packetDetailDTO.setTotalBytes(packetDetailDTOFromMap.getTotalBytes()
				+ bin + bout);

		usageMap.put(split[0], packetDetailDTO);

		PacketDetailDTO tdrPacketDetailDTO = new PacketDetailDTO();
		tdrPacketDetailDTO.setPhoneNumber(phoneNumber);
		tdrPacketDetailDTO.setBin(bin);
		tdrPacketDetailDTO.setBout(bout);
		tdrPacketDetailDTO.setTimestamp(split[3]);

		collector.emit("usagestream", new Values(packetDetailDTO));
		collector.emit("tdrstream", new Values(tdrPacketDetailDTO));
	}

	@Override
	public void cleanup() {
		client.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("usagestream", new Fields("usagestream"));
		declarer.declareStream("tdrstream", new Fields("tdrstream"));
	}

}
