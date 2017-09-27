package com.book.chapter8.basic;

import java.util.Map;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

public class DisplayOutputFilter implements Filter {
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
	@Override
	public boolean isKeep(TridentTuple tuple) {
		System.out.print("[");
		for (int index = 0 ; index < tuple.size(); index ++){
			System.out.print(tuple.get(index));
			if(index < (tuple.size()-1))
			System.out.print(",");
		}
		System.out.println("]");
		return true;
	}
}
