package com.book.logstash.diy;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;

public class PubNubDataStream {

	public KafkaProducer<Integer, String> producer;

	public PubNubDataStream() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("acks", "1");
		producer = new KafkaProducer<Integer, String>(properties);
	}

	private void pubishMessageToKafka(String message) {
		ProducerRecord<Integer, String> data = new ProducerRecord<Integer, String>(
				"logstash-diy-example", message);
		producer.send(data);
	}

	private void getMessageFromPubNub() {
		PNConfiguration pnConfiguration = new PNConfiguration();
		pnConfiguration
				.setSubscribeKey("sub-c-5f1b7c8e-fbee-11e3-aa40-02ee2ddab7fe");
		PubNub pubnub = new PubNub(pnConfiguration);

		pubnub.addListener(new SubscribeCallback() {
			@Override
			public void status(PubNub pubnub, PNStatus status) {

				System.out.println(pubnub.toString() + "::" + status.toString());

				if (status.getCategory() == PNStatusCategory.PNUnexpectedDisconnectCategory) {
					// This event happens when radio / connectivity is lost
				}

				else if (status.getCategory() == PNStatusCategory.PNConnectedCategory) {
					// Connect event. You can do stuff like publish, and know
					// you'll get it.
					// Or just use the connected event to confirm you are
					// subscribed for
					// UI / internal notifications, etc
					if (status.getCategory() == PNStatusCategory.PNConnectedCategory) {
						System.out.println("status.getCategory()="
								+ status.getCategory());
					}
				} else if (status.getCategory() == PNStatusCategory.PNReconnectedCategory) {
					// Happens as part of our regular operation. This event
					// happens when
					// radio / connectivity is lost, then regained.
				} else if (status.getCategory() == PNStatusCategory.PNDecryptionErrorCategory) {
					// Handle messsage decryption error. Probably client
					// configured to
					// encrypt messages and on live data feed it received plain
					// text.
				}
			}

			@Override
			public void message(PubNub pubnub, PNMessageResult message) {
				// Handle new message stored in message.message
				String strMessage = message.getMessage().toString();
				System.out.println("******" + strMessage);
				pubishMessageToKafka(strMessage);

				/*
				 * log the following items with your favorite logger -
				 * message.getMessage() - message.getSubscription() -
				 * message.getTimetoken()
				 */
			}

			@Override
			public void presence(PubNub pubnub, PNPresenceEventResult presence) {
			}
		});

		pubnub.subscribe().channels(Arrays.asList("pubnub-sensor-network"))
				.execute();
	}

	public static void main(String[] args) {
		new PubNubDataStream().getMessageFromPubNub();
	}
}
