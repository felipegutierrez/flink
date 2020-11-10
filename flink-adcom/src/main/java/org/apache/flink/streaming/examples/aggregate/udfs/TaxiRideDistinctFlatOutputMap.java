package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class TaxiRideDistinctFlatOutputMap implements MapFunction<Tuple3<Integer, Long, Long>, String> {
	@Override
	public String map(Tuple3<Integer, Long, Long> value) {
		return value.f0 + " - " + value.f1 + " - " + value.f2;
	}
}
