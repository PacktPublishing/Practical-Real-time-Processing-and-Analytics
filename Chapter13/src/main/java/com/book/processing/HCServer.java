package com.book.processing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;

public class HCServer {
	public static void main(String args[]) {
		Hazelcast.newHazelcastInstance(new Config());
	}
}
