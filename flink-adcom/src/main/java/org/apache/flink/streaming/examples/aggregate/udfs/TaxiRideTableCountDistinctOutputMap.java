package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideTableCountDistinctOutputMap implements MapFunction<Tuple2<Boolean, Tuple2<String, Long>>, String> {

	@Override
	public String map(Tuple2<Boolean, Tuple2<String, Long>> value) throws Exception {
		return value.f0 + "|date: " + value.f1.f0 + "| taxi driver count distinct: " + value.f1.f1;
	}
}
