package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.PreAggregateConcurrentFunction;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.MqttDataSink;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSource;
import org.apache.flink.streaming.examples.aggregate.util.*;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

/**
 * <pre>
 * -controller true -pre-aggregate-window 1 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -output mqtt -sinkHost 127.0.0.1
 *
 * -controller false -pre-aggregate-window 100 -pre-aggregate-window-timeout 1 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -output mqtt -sinkHost 127.0.0.1
 * </pre>
 */
public class TaxiRideCountPreAggregate {
	public static void main(String[] args) throws Exception {
		// @formatter:off
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, ExerciseBase.pathToRideData);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, "");
		int timeCharacteristic = params.getInt(TIME_CHARACTERISTIC, 0);
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 0);
		long preAggregationWindowTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);

		System.out.println("Download data from:");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiRides.gz");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiFares.gz");
		System.out.println("data source                                             : " + input);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("time characteristic 1-Processing 2-Event 3-Ingestion    : " + timeCharacteristic);
		System.out.println("pre-aggregate window [count]                            : " + preAggregationWindowCount);
		System.out.println("pre-aggregate window [seconds]                          : " + preAggregationWindowTimer);
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
		// @formatter:on

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (timeCharacteristic != 0) {
			if (timeCharacteristic == 1) {
				env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
			} else if (timeCharacteristic == 2) {
				env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			} else if (timeCharacteristic == 3) {
				env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			}
		}

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
		DataStream<Tuple2<Long, Long>> tuples = rides
			.map(new TokenizerMap())
			.name(OPERATOR_TOKENIZER)
			.uid(OPERATOR_TOKENIZER)
			.slotSharingGroup(slotGroup01);

		DataStream<Tuple2<Long, Long>> preAggregatedStream = null;
		if (preAggregationWindowCount == 0 && preAggregationWindowTimer == -1) {
			// no combiner
			preAggregatedStream = tuples;
		} else if (enableController == false && preAggregationWindowTimer > 0) {
			// static combiner based on timeout
			PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
			preAggregatedStream = tuples
				.combine(taxiRidePreAggregateFunction, preAggregationWindowTimer)
				.name(OPERATOR_PRE_AGGREGATE)
				.uid(OPERATOR_PRE_AGGREGATE)
				.disableChaining()
				.slotSharingGroup(slotGroup01);
		} else if (enableController == false && preAggregationWindowCount > 0) {
			// static combiner based on number of tuples
//			PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
//			preAggregatedStream = tuples
//				.combine(taxiRidePreAggregateFunction, preAggregationWindowCount)
//				.name(OPERATOR_PRE_AGGREGATE)
//				.uid(OPERATOR_PRE_AGGREGATE)
//				.disableChaining()
//				.slotSharingGroup(slotGroup01);
		} else {
			// dynamic combiner with PI controller
//			PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
//			preAggregatedStream = tuples
//				.adCombine(taxiRidePreAggregateFunction)
//				.name(OPERATOR_PRE_AGGREGATE)
//				.uid(OPERATOR_PRE_AGGREGATE)
//				.disableChaining()
//				.slotSharingGroup(slotGroup01);
		}

		KeyedStream<Tuple2<Long, Long>, Tuple> keyedByDriverId = preAggregatedStream.keyBy(0);

		DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId
			.reduce(new SumReduceFunction())
			.name(OPERATOR_REDUCER)
			.uid(OPERATOR_REDUCER)
			.slotSharingGroup(slotGroup02)
			.setParallelism(parallelisGroup02);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			rideCounts
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
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			rideCounts
				.print()
				.name(OPERATOR_SINK)
				.uid(OPERATOR_SINK)
				.slotSharingGroup(slotGroup02)
				.setParallelism(parallelisGroup02);
		} else {
			System.out.println("discarding output");
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideCountPreAggregate.class.getSimpleName());
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	private static class TokenizerMap implements MapFunction<TaxiRide, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(TaxiRide ride) {
			return new Tuple2<Long, Long>(ride.driverId, 1L);
		}
	}

	private static class TokenizerTimeMap implements MapFunction<TaxiRide, Tuple3<Long, Long, Long>> {
		@Override
		public Tuple3<Long, Long, Long> map(TaxiRide ride) {
			return new Tuple3<Long, Long, Long>(ride.driverId, 1L, 0L);
		}
	}

	private static class TaxiRideCountPreAggregateFunction
		extends PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Long addInput(@Nullable Long value, Tuple2<Long, Long> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				return value + input.f1;
			}
		}

		@Override
		public void collect(Map<Long, Long> buffer, Collector<Tuple2<Long, Long>> out) {
			for (Map.Entry<Long, Long> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class TaxiRideTimeCountPreAggregateFunction
		extends PreAggregateFunction<Long, Tuple2<Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {

		@Override
		public Tuple2<Long, Long> addInput(
			@Nullable Tuple2<Long, Long> value,
			Tuple3<Long, Long, Long> input) throws Exception {
			if (value == null) {
				return Tuple2.of(input.f1, input.f2);
			} else {
				// return only the largest timestamp
				if (input.f2 >= value.f1) {
					return Tuple2.of(value.f0 + input.f1, input.f2);
				} else {
					return Tuple2.of(value.f0 + input.f1, value.f1);
				}
			}
		}

		@Override
		public void collect(
			Map<Long, Tuple2<Long, Long>> buffer,
			Collector<Tuple3<Long, Long, Long>> out) {
			for (Map.Entry<Long, Tuple2<Long, Long>> entry : buffer.entrySet()) {
				out.collect(Tuple3.of(entry.getKey(), entry.getValue().f0, entry.getValue().f1));
			}
		}
	}

	private static class TaxiRideCountPreAggregateConcurrentFunction
		extends PreAggregateConcurrentFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Long addInput(@Nullable Long value, Tuple2<Long, Long> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				return value + input.f1;
			}
		}

		@Override
		public void collect(ConcurrentMap<Long, Long> buffer, Collector<Tuple2<Long, Long>> out) {
			for (Map.Entry<Long, Long> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class TaxiRideTimeCountPreAggregateConcurrentFunction
		extends PreAggregateConcurrentFunction<Long, Tuple2<Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {

		@Override
		public Tuple2<Long, Long> addInput(
			@Nullable Tuple2<Long, Long> value,
			Tuple3<Long, Long, Long> input) throws Exception {
			if (value == null) {
				return Tuple2.of(input.f1, input.f2);
			} else {
				// return only the largest timestamp
				if (input.f2 >= value.f1) {
					return Tuple2.of(value.f0 + input.f1, input.f2);
				} else {
					return Tuple2.of(value.f0 + input.f1, value.f1);
				}
			}
		}

		@Override
		public void collect(
			ConcurrentMap<Long, Tuple2<Long, Long>> buffer,
			Collector<Tuple3<Long, Long, Long>> out) {
			for (Map.Entry<Long, Tuple2<Long, Long>> entry : buffer.entrySet()) {
				out.collect(Tuple3.of(entry.getKey(), entry.getValue().f0, entry.getValue().f1));
			}
		}
	}

	private static class SumReduceFunction implements ReduceFunction<Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
			return Tuple2.of(value1.f0, value1.f1 + value2.f1);
		}
	}

	private static class SumTimeReduceFunction implements ReduceFunction<Tuple3<Long, Long, Long>> {
		@Override
		public Tuple3<Long, Long, Long> reduce(
			Tuple3<Long, Long, Long> value1,
			Tuple3<Long, Long, Long> value2) {
			// return only the largest timestamp
			if (value1.f2 >= value2.f2) {
				return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2);
			} else {
				return Tuple3.of(value1.f0, value1.f1 + value2.f1, value2.f2);
			}
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<Long, Long>, String> {
		@Override
		public String map(Tuple2<Long, Long> value) {
			return value.f0 + " - " + value.f1;
		}
	}

	private static class FlatTimeOutputMap implements MapFunction<Tuple3<Long, Long, Long>, String> {
		@Override
		public String map(Tuple3<Long, Long, Long> value) {
			return value.f0 + "-" + value.f1 + "-" + value.f2;
		}
	}
}
