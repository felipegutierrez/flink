package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.streaming.examples.aggregate.util.GenericParameters;

public class TaxiRideAveragePreAggregate {
	public static void main(String[] args) throws Exception {
		GenericParameters genericParam = new GenericParameters(args);
		genericParam.printParameters();

		/*
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}
		String slotGroup01 = SLOT_GROUP_DEFAULT;
		String slotGroup02 = SLOT_GROUP_DEFAULT;
		if (slotSplit == 0) {
			slotGroup01 = SLOT_GROUP_DEFAULT;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (slotSplit == 1) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (slotSplit == 2) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_01_02;
		}

		DataStream<TaxiRide> rides = env
			.addSource(new TaxiRideSource(input))
			.name(OPERATOR_SOURCE)
			.uid(OPERATOR_SOURCE)
			.slotSharingGroup(slotGroup01);

		if (!enableCombiner) {
			DataStream<Tuple5<Integer, Double, Double, Double, Long>> tuples = rides
				.map(new TokenizerNoCombinerMap())
				.name(OPERATOR_TOKENIZER)
				.uid(OPERATOR_TOKENIZER)
				.slotSharingGroup(slotGroup01);

			// no combiner
			KeyedStream<Tuple5<Integer, Double, Double, Double, Long>, Integer> keyedStream = tuples
				.keyBy(new RandomDriverNoCombinerKeySelector());

			DataStream<Tuple5<Integer, Double, Double, Double, Long>> averagePassengers = keyedStream
				.reduce(new SumPassengersNoCombinerReducer())
				.name(OPERATOR_REDUCER)
				.uid(OPERATOR_REDUCER)
				.slotSharingGroup(slotGroup02)
				.setParallelism(parallelisGroup02);

			if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
				averagePassengers
					.map(new FlatOutputAverageNoCombinerMap())
					.name(OPERATOR_FLAT_OUTPUT)
					.uid(OPERATOR_FLAT_OUTPUT)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02)
					.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort))
					.name(OPERATOR_SINK)
					.uid(OPERATOR_SINK)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02);
			} else {
				averagePassengers
					.map(new FlatOutputAverageNoCombinerMap())
					.name(OPERATOR_FLAT_OUTPUT)
					.uid(OPERATOR_FLAT_OUTPUT)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02)
					.print()
					.name(OPERATOR_SINK)
					.uid(OPERATOR_SINK)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02);
			}
		} else {
			DataStream<Tuple4<Integer, Double, Double, Double>> tuples = rides
				.map(new TokenizerMap())
				.name(OPERATOR_TOKENIZER)
				.uid(OPERATOR_TOKENIZER)
				.slotSharingGroup(slotGroup01);

			// static and autonomous combiner
			DataStream<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> preAggregatedStream = null;
			if (enableController == false && preAggregationWindowTimer > 0) {
				// static combiner based on timeout
				PreAggregateFunction<Integer,
					Tuple2<Integer, Tuple4<Double, Double, Double, Long>>,
					Tuple4<Integer, Double, Double, Double>,
					Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> taxiRidePreAggregateFunction = new TaxiRideSumPreAggregateFunction();

				preAggregatedStream = tuples
					.combine(taxiRidePreAggregateFunction, preAggregationWindowTimer)
					.disableChaining()
					.name(OPERATOR_PRE_AGGREGATE)
					.uid(OPERATOR_PRE_AGGREGATE)
					.slotSharingGroup(slotGroup01);
			} else if (enableController == false && preAggregationWindowCount > 0) {
				// static combiner based on number of records
				PreAggregateFunction<Integer,
					Tuple2<Integer, Tuple4<Double, Double, Double, Long>>,
					Tuple4<Integer, Double, Double, Double>,
					Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> taxiRidePreAggregateFunction = new TaxiRideSumPreAggregateFunction();

				preAggregatedStream = tuples
					.combine(taxiRidePreAggregateFunction, preAggregationWindowCount)
					.disableChaining()
					.name(OPERATOR_PRE_AGGREGATE)
					.uid(OPERATOR_PRE_AGGREGATE)
					.slotSharingGroup(slotGroup01);
			} else {
//				PreAggregateFunction<Integer,
//					Tuple2<Integer, Tuple4<Double, Double, Double, Long>>,
//					Tuple4<Integer, Double, Double, Double>,
//					Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> taxiRidePreAggregateFunction = new TaxiRideSumPreAggregateFunction();
//
//				preAggregatedStream = tuples
//					.adCombine(taxiRidePreAggregateFunction)
//					.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
			}

			KeyedStream<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>, Integer> keyedByRandomDriver = preAggregatedStream
				.keyBy(new RandomDriverCombinerKeySelector());

			DataStream<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> averagePassengers = keyedByRandomDriver
				.reduce(new AveragePassengersReducer())
				.name(OPERATOR_REDUCER)
				.uid(OPERATOR_REDUCER)
				.slotSharingGroup(slotGroup02)
				.setParallelism(parallelisGroup02);

			if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
				averagePassengers
					.map(new FlatOutputMap())
					.name(OPERATOR_FLAT_OUTPUT)
					.uid(OPERATOR_FLAT_OUTPUT)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02)
					.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort))
					.name(OPERATOR_SINK)
					.uid(OPERATOR_SINK)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02);
			} else {
				averagePassengers
					.map(new FlatOutputMap())
					.name(OPERATOR_FLAT_OUTPUT)
					.uid(OPERATOR_FLAT_OUTPUT)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02)
					.print()
					.name(OPERATOR_SINK)
					.uid(OPERATOR_SINK)
					.slotSharingGroup(slotGroup02)
					.setParallelism(parallelisGroup02);
			}
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideAveragePreAggregate.class.getSimpleName());

		 */
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	/*
	private static class TokenizerMap implements MapFunction<TaxiRide, Tuple4<Integer, Double, Double, Double>> {
		private final Random random;

		public TokenizerMap() {
			random = new Random();
		}

		@Override
		public Tuple4<Integer, Double, Double, Double> map(TaxiRide ride) {
			// passengers on the taxi ride
			Double passengerCnt = Double.valueOf(ride.passengerCnt);

			// create random keys from 0 to 10 in order to average the passengers of all taxi drivers
			int low = 0;
			int high = 10;
			Integer randomKey = random.nextInt(high - low) + low;
			Double distance = TaxiRideDistanceCalculator.distance(
				ride.startLat,
				ride.startLon,
				ride.endLat,
				ride.endLon,
				"K");

			// elapsed time taxi ride
//			long elapsedTimeMilliSec = ride.endTime.getMillis() - ride.startTime.getMillis();
			long elapsedTimeMilliSec = 1L;
			Double elapsedTimeMinutes = Double.valueOf(elapsedTimeMilliSec * 1000 * 60);

			return Tuple4.of(randomKey, passengerCnt, distance, elapsedTimeMinutes);
		}
	}

	private static class TokenizerNoCombinerMap implements MapFunction<TaxiRide, Tuple5<Integer, Double, Double, Double, Long>> {
		private final Random random;

		public TokenizerNoCombinerMap() {
			random = new Random();
		}

		@Override
		public Tuple5<Integer, Double, Double, Double, Long> map(TaxiRide ride) {
			// passengers on the taxi ride
			Double passengerCnt = Double.valueOf(ride.passengerCnt);

			// create random keys from 0 to 10 in order to average the passengers of all taxi drivers
			int low = 0;
			int high = 10;
			Integer randomKey = random.nextInt(high - low) + low;
			Double distance = TaxiRideDistanceCalculator.distance(
				ride.startLat,
				ride.startLon,
				ride.endLat,
				ride.endLon,
				"K");

			// elapsed time taxi ride
			// long elapsedTimeMilliSec = ride.endTime.getMillis() - ride.startTime.getMillis();
			long elapsedTimeMilliSec = 1L;
			Double elapsedTimeMinutes = Double.valueOf(elapsedTimeMilliSec * 1000 * 60);

			return Tuple5.of(randomKey, passengerCnt, distance, elapsedTimeMinutes, 1L);
		}
	}

	 */

	/**
	 * Count the number of values and sum them.
	 * Key (Integer): random-key
	 * Value (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
	 * Input (Integer, Double): random-key, passengerCnt
	 * Output (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
	 */
	/*
	private static class TaxiRideSumPreAggregateFunction
		extends PreAggregateFunction<Integer,
		Tuple2<Integer, Tuple4<Double, Double, Double, Long>>,
		Tuple4<Integer, Double, Double, Double>,
		Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> {

		@Override
		public Tuple2<Integer, Tuple4<Double, Double, Double, Long>> addInput(
			@Nullable Tuple2<Integer, Tuple4<Double, Double, Double, Long>> value,
			Tuple4<Integer, Double, Double, Double> input) throws Exception {
			Integer randomKey = input.f0;
			if (value == null) {
				Double passengers = input.f1;
				Double distances = input.f2;
				Double elapsedTimeMinutes = input.f3;
				return Tuple2.of(
					randomKey,
					Tuple4.of(passengers, distances, elapsedTimeMinutes, 1L));
			} else {
				Double passengers = input.f1 + value.f1.f0;
				Double distances = input.f2 + value.f1.f1;
				Double elapsedTimeMinutes = input.f3 + value.f1.f2;
				Long count = value.f1.f3 + 1;
				return Tuple2.of(
					randomKey,
					Tuple4.of(passengers, distances, elapsedTimeMinutes, count));
			}
		}

		@Override
		public void collect(
			Map<Integer, Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> buffer,
			Collector<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> out) throws Exception {
			for (Map.Entry<Integer, Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> entry : buffer
				.entrySet()) {
				Double passengers = entry.getValue().f1.f0;
				Double distances = entry.getValue().f1.f1;
				Double elapsedTimeMinutes = entry.getValue().f1.f2;
				Long driverIdCount = entry.getValue().f1.f3;
				out.collect(Tuple2.of(
					entry.getKey(),
					Tuple4.of(passengers, distances, elapsedTimeMinutes, driverIdCount)));
			}
		}
	}

	private static class TaxiRideSumPreAggregateConcurrentFunction
		extends PreAggregateConcurrentFunction<Integer,
		Tuple2<Integer, Tuple4<Double, Double, Double, Long>>,
		Tuple4<Integer, Double, Double, Double>,
		Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> {

		@Override
		public Tuple2<Integer, Tuple4<Double, Double, Double, Long>> addInput(
			@Nullable Tuple2<Integer, Tuple4<Double, Double, Double, Long>> value,
			Tuple4<Integer, Double, Double, Double> input) throws Exception {
			Integer randomKey = input.f0;
			if (value == null) {
				Double passengers = input.f1;
				Double distances = input.f2;
				Double elapsedTimeMinutes = input.f3;
				return Tuple2.of(
					randomKey,
					Tuple4.of(passengers, distances, elapsedTimeMinutes, 1L));
			} else {
				Double passengers = input.f1 + value.f1.f0;
				Double distances = input.f2 + value.f1.f1;
				Double elapsedTimeMinutes = input.f3 + value.f1.f2;
				Long count = value.f1.f3 + 1;
				return Tuple2.of(
					randomKey,
					Tuple4.of(passengers, distances, elapsedTimeMinutes, count));
			}
		}

		@Override
		public void collect(
			ConcurrentMap<Integer, Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> buffer,
			Collector<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> out) throws Exception {
			for (Map.Entry<Integer, Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> entry : buffer
				.entrySet()) {
				Double passengers = entry.getValue().f1.f0;
				Double distances = entry.getValue().f1.f1;
				Double elapsedTimeMinutes = entry.getValue().f1.f2;
				Long driverIdCount = entry.getValue().f1.f3;
				out.collect(Tuple2.of(
					entry.getKey(),
					Tuple4.of(passengers, distances, elapsedTimeMinutes, driverIdCount)));
			}
		}
	}

	private static class RandomDriverCombinerKeySelector implements KeySelector<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>, Integer> {
		@Override
		public Integer getKey(Tuple2<Integer, Tuple4<Double, Double, Double, Long>> value) throws Exception {
			return value.f0;
		}
	}

	private static class RandomDriverNoCombinerKeySelector implements KeySelector<Tuple5<Integer, Double, Double, Double, Long>, Integer> {
		@Override
		public Integer getKey(Tuple5<Integer, Double, Double, Double, Long> value) throws Exception {
			return value.f0;
		}
	}


	private static class AveragePassengersReducer implements ReduceFunction<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>> {
		@Override
		public Tuple2<Integer, Tuple4<Double, Double, Double, Long>> reduce(
			Tuple2<Integer, Tuple4<Double, Double, Double, Long>> value1,
			Tuple2<Integer, Tuple4<Double, Double, Double, Long>> value2) throws Exception {
			Integer randomKey = value1.f0;
			Long driverIdCount = value1.f1.f3 + value2.f1.f3;

			Double passengers = (value1.f1.f0 + value2.f1.f0) / driverIdCount;
			Double distances = (value1.f1.f1 + value2.f1.f1) / driverIdCount;
			Double elapsedTimeMinutes = (value1.f1.f2 + value2.f1.f2) / driverIdCount;

			return Tuple2.of(randomKey, Tuple4.of(passengers, distances, elapsedTimeMinutes, 1L));
		}
	}

	private static class SumPassengersNoCombinerReducer implements ReduceFunction<Tuple5<Integer, Double, Double, Double, Long>> {
		@Override
		public Tuple5<Integer, Double, Double, Double, Long> reduce(
			Tuple5<Integer, Double, Double, Double, Long> value1,
			Tuple5<Integer, Double, Double, Double, Long> value2) throws Exception {
			Integer randomKey = value1.f0;
			Long driverIdCount = value1.f4 + value2.f4;
			Double passengers = (value1.f1 + value2.f1);
			Double distances = (value1.f2 + value2.f2);
			Double elapsedTimeMinutes = (value1.f3 + value2.f3);
			return Tuple5.of(randomKey, passengers, distances, elapsedTimeMinutes, driverIdCount);
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<Integer, Tuple4<Double, Double, Double, Long>>, String> {
		@Override
		public String map(Tuple2<Integer, Tuple4<Double, Double, Double, Long>> value) {
			return "Average values: passengers[" + value.f1.f0 + "] distance(Km)[" + value.f1.f1
				+ "] minutes[" + value.f1.f2 + "]";
		}
	}

	private static class FlatOutputAverageNoCombinerMap implements MapFunction<Tuple5<Integer, Double, Double, Double, Long>, String> {
		@Override
		public String map(Tuple5<Integer, Double, Double, Double, Long> value) throws Exception {
			Long taxiRideSum = value.f4;
			Double passengersAvg = value.f1 / taxiRideSum;
			Double distanceAvg = value.f2 / taxiRideSum;
			Double tripTimeAvg = value.f3 / taxiRideSum;
			return "Average values: passengers[" + passengersAvg + "] distance(Km)[" + distanceAvg
				+ "] minutes[" + tripTimeAvg + "]";
		}
	}
	 */
}
