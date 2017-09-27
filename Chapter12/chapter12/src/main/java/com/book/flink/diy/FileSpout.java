package com.book.flink.diy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileSpout extends BaseRichSpout {
	private static final long serialVersionUID = -6167039596158642349L;
	private SpoutOutputCollector collector;
	private String fileName;
	private BufferedReader reader;

	public FileSpout(String fileName) {
		this.fileName = fileName;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		//fileName = (String) conf.get("file");
		this.collector = collector;

		try {
			reader = new BufferedReader(new FileReader(fileName));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {
		try {
			String line = reader.readLine();
			if (line != null) {
				collector.emit(new Values(line));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields schema = new Fields("line");
		declarer.declare(schema);
	}
}