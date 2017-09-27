package com.book.processing;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.book.domain.AlertEvent;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

/**
 * This bolt is used to generate alert in form of SMS using Twilio application.
 * Also emit tuples so that it can be saved into Elasticsearch.
 * 
 * @author SGupta
 *
 */
public class GenerateAlertBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6802250427993673417L;
	private Map<String, AlertEvent> vehicleAlertMap;
	private HazelcastInstance hazelcastClient;

	private String host;
	private String port;

	public GenerateAlertBolt(String host, String port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getNetworkConfig().addAddress(host + ":" + port);
		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
		vehicleAlertMap = hazelcastClient.getMap("generatedAlerts");
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// Get alert event from checkDistanceAndAlert bolt.
		AlertEvent alertEvent = (AlertEvent) input.getValueByField("alertInfo");

		// Reading map containing alerts from Hazelcast
		AlertEvent previousAlertEvent = vehicleAlertMap.get(alertEvent.getVehicleId());

		// Check whether alert is already generated for this vehicle or not.
		if (previousAlertEvent == null) {
			// Add entry in Hazelcast Map
			vehicleAlertMap.put(alertEvent.getVehicleId(), alertEvent);
			System.out.println(alertEvent.toString());
			String message = "ALERT!! Hi, your vehicle id " + alertEvent.getVehicleId()
					+ " is moving out of start location i.e. ("
					+ BigDecimal.valueOf(alertEvent.getActualLatitude()).setScale(2, RoundingMode.HALF_DOWN)
							.doubleValue()
					+ ","
					+ BigDecimal.valueOf(alertEvent.getActualLongitude()).setScale(2, RoundingMode.HALF_DOWN)
							.doubleValue()
					+ ") with distance " + BigDecimal.valueOf(alertEvent.getActualDistance())
							.setScale(2, RoundingMode.HALF_DOWN).doubleValue();
			System.out.println(" GenerateAlertBOLT: >>>>> " + message);
			// Generate SMS.
			sendMessage(alertEvent.getPhoneNumber(), message);

			// Emit tuple for next bolt.
			collector.emit("generatedAlertInfo", new Values(alertEvent));
		} else {
			System.out.println(" GenerateAlertBOLT: >>>>> Alert is already generated for " + alertEvent.getVehicleId());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("generatedAlertInfo", new Fields("generatedAlertInfo"));
	}

	public void sendMessage(String phoneNumber, String message) {
		String s;
		Process p;
		// TODO replace YOWSUP home path with YOWSUP
		String YOWSUP_HOME="/home/impadmin/git/yowsup/";
		try {
			String[] sendCommand = { "python", YOWSUP_HOME+"yowsup-cli", "demos", "-c", YOWSUP_HOME+"whatsapp_config.txt", "-s", phoneNumber, message };
			p = Runtime.getRuntime().exec(sendCommand);
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((s = br.readLine()) != null)
				System.out.println("line: " + s);

			BufferedReader errBr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while ((s = errBr.readLine()) != null)
				System.out.println("line: " + s);

			p.waitFor();
			System.out.println("exit: " + p.exitValue());
			p.destroy();
		} catch (Exception e) {
		}
	}
}
