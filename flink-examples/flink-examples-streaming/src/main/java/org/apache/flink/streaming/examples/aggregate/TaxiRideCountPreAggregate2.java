package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateStrategy;
import org.apache.flink.streaming.examples.aggregate.util.ExerciseBase;
import org.apache.flink.streaming.examples.aggregate.util.MqttDataSink;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRideSource;
import org.apache.flink.streaming.examples.utils.DataRateListener;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Map;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

public class TaxiRideCountPreAggregate2 {
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, ExerciseBase.pathToRideData);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, "");
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 0);
		int parallelisGroup01 = params.getInt(PARALLELISM_GROUP_01, ExecutionConfig.PARALLELISM_DEFAULT);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean slotSplit = params.getBoolean(SLOT_GROUP_SPLIT, false);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);
		PreAggregateStrategy preAggregateStrategy = PreAggregateStrategy.valueOf(params.get(PRE_AGGREGATE_STRATEGY,
			PreAggregateStrategy.GLOBAL.toString()));

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served every second

		System.out.println("Download data from:");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiRides.gz");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiFares.gz");
		System.out.println("data source                              : " + input);
		System.out.println("data sink                                : " + output);
		System.out.println("data sink host:port                      : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                          : " + TOPIC_DATA_SINK);
		System.out.println("Feedback loop Controller                 : " + enableController);
		System.out.println("Splitting into different slots           : " + slotSplit);
		System.out.println("Parallelism group 01                     : " + parallelisGroup01);
		System.out.println("Parallelism group 02                     : " + parallelisGroup02);
		System.out.println("Disable operator chaining                : " + disableOperatorChaining);
		System.out.println("pre-aggregate window [count]             : " + preAggregationWindowCount);
		System.out.println("pre-aggregate strategy                   : " + preAggregateStrategy.getValue());
		System.out.println("Changing pre-aggregation frequency before shuffling:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-pre-aggregate-parameter -m \"100\"");
		System.out.println(DataRateListener.class.getSimpleName() + " class to read data rate from file [" + DataRateListener.DATA_RATE_FILE + "] in milliseconds.");
		System.out.println("This listener reads every 60 seconds only the first line from the data rate file.");
		System.out.println("Use the following command to change the nanoseconds data rate:");
		System.out.println("1000000 nanoseconds = 1 millisecond");
		System.out.println("1000000000 nanoseconds = 1000 milliseconds = 1 second");
		System.out.println("echo \"1000000000\" > " + DataRateListener.DATA_RATE_FILE);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}
		String slotGroup01_1 = "default";
		String slotGroup01_2 = "default";
		String slotGroup02_1 = "default";
		String slotGroup02_2 = "default";
		if (slotSplit) {
			slotGroup01_1 = SLOT_GROUP_01_01;
			slotGroup01_2 = SLOT_GROUP_01_02;
			slotGroup02_1 = SLOT_GROUP_02_01;
			slotGroup02_2 = SLOT_GROUP_02_02;
		}

		DataStream<TaxiRide> rides01 = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)).name(OPERATOR_SOURCE).slotSharingGroup(slotGroup01_1);
		DataStream<TaxiRide> rides02 = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)).name(OPERATOR_SOURCE).slotSharingGroup(slotGroup01_2);
		DataStream<Tuple2<Long, Long>> tuples01 = rides01.map(new TokenizerMap()).name(OPERATOR_TOKENIZER).setParallelism(parallelisGroup01).slotSharingGroup(slotGroup01_1);
		DataStream<Tuple2<Long, Long>> tuples02 = rides02.map(new TokenizerMap()).name(OPERATOR_TOKENIZER).setParallelism(parallelisGroup01).slotSharingGroup(slotGroup01_2);

		DataStream<Tuple2<Long, Long>> preAggregatedStream01 = null;
		DataStream<Tuple2<Long, Long>> preAggregatedStream02 = null;
		PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
		if (preAggregationWindowCount == 0) {
			// NO PRE_AGGREGATE
			preAggregatedStream01 = tuples01;
			preAggregatedStream02 = tuples02;
		} else {
			preAggregatedStream01 = tuples01
				.combiner(taxiRidePreAggregateFunction, preAggregationWindowCount, enableController, preAggregateStrategy)
				.name(OPERATOR_PRE_AGGREGATE).disableChaining().setParallelism(parallelisGroup01).slotSharingGroup(slotGroup01_1);
			preAggregatedStream02 = tuples02
				.combiner(taxiRidePreAggregateFunction, preAggregationWindowCount, enableController, preAggregateStrategy)
				.name(OPERATOR_PRE_AGGREGATE).disableChaining().setParallelism(parallelisGroup01).slotSharingGroup(slotGroup01_2);
		}

		KeyedStream<Tuple2<Long, Long>, Tuple> keyedByDriverId = preAggregatedStream01.union(preAggregatedStream02).keyBy(0);

		DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId
			.reduce(new SumReduceFunction()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).setParallelism(parallelisGroup02).slotSharingGroup(slotGroup02_1);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			rideCounts
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).setParallelism(parallelisGroup02).slotSharingGroup(slotGroup02_1)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).setParallelism(parallelisGroup02).slotSharingGroup(slotGroup02_1);
		} else {
			rideCounts.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).setParallelism(parallelisGroup02).slotSharingGroup(slotGroup02_1);
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideCountPreAggregate2.class.getSimpleName());
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
		public void collect(Map<Long, Long> buffer, Collector<Tuple2<Long, Long>> out) throws Exception {
			for (Map.Entry<Long, Long> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class SumReduceFunction implements ReduceFunction<Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
			return Tuple2.of(value1.f0, value1.f1 + value2.f1);
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<Long, Long>, String> {
		@Override
		public String map(Tuple2<Long, Long> value) {
			return value.f0 + " - " + value.f1;
		}
	}
}
