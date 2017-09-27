package com.book.realtime.job;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FixedSentenceSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	// Set of sentences
	private String[] sentences = { "This is example of chapter 4", "This is word count example", "Very basic example of Apache Storm",
			"Apache Storm is open source real time processing engine"};
	private int index = 0;

	//Declare output field which would be input for next spout or bolt
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	//Emitting tuple to next bolt
	public void nextTuple() {
		String sentence = sentences[index];
		
		System.out.println(sentence);
		
		this.collector.emit(new Values(sentence));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}