package com.book.chapter7.diy;

import com.hazelcast.core.Hazelcast;

public class HCServer {
	public static void main(String args[]) {
		Hazelcast.newHazelcastInstance();
	}
}
