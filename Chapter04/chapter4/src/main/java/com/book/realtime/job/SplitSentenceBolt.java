package com.book.realtime.job;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	public void prepare(@SuppressWarnings("rawtypes") Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word : words) {
			this.collector.emit(new Values(word));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}