package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideAveragePassengersReducer implements ReduceFunction<Tuple2<Integer, Tuple2<Double, Long>>> {
	@Override
	public Tuple2<Integer, Tuple2<Double, Long>> reduce(
		Tuple2<Integer, Tuple2<Double, Long>> value1,
		Tuple2<Integer, Tuple2<Double, Long>> value2) throws Exception {
		Integer randomKey = value1.f0;
		Double distanceSum = value1.f1.f0 + value2.f1.f0;
		Long count = value1.f1.f1 + value2.f1.f1;
		Double distanceAvg = distanceSum / count;
		return Tuple2.of(randomKey, Tuple2.of(distanceAvg, 1L));
	}
}
