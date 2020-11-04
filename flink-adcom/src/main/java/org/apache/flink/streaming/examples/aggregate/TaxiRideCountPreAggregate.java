package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.MqttDataSink;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideCountPreAggregateFunction;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideFlatOutputMap;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideKeySelector;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSource;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSourceParallel;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSumReduceFunction;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideDriverTokenizerMap;
import org.apache.flink.streaming.examples.aggregate.util.ExerciseBase;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.CONTROLLER;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.DISABLE_OPERATOR_CHAINING;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_FLAT_OUTPUT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_PRE_AGGREGATE;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_REDUCER;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_SINK;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_SOURCE;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_TOKENIZER;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.PARALLELISM_GROUP_02;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.PRE_AGGREGATE_WINDOW_TIMEOUT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SINK;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SINK_DATA_MQTT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SINK_HOST;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SINK_PORT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SINK_TEXT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SLOT_GROUP_01_01;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SLOT_GROUP_01_02;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SLOT_GROUP_DEFAULT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SLOT_GROUP_SPLIT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SOURCE;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SOURCE_PARALLEL;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.TIME_CHARACTERISTIC;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.TOPIC_DATA_SINK;

/**
 * <pre>
 * -controller true -pre-aggregate-window 1 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1
 *
 * -controller false -pre-aggregate-window 100 -pre-aggregate-window-timeout 1 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1
 * </pre>
 */
public class TaxiRideCountPreAggregate {
	public static void main(String[] args) throws Exception {
		// @formatter:off
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, ExerciseBase.pathToRideData);
		boolean parallelSource = params.getBoolean(SOURCE_PARALLEL, false);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, "");
		int timeCharacteristic = params.getInt(TIME_CHARACTERISTIC, 0);
		long preAggregationProcessingTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);

		System.out.println("Download data from:");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiRides.gz");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiFares.gz");
		System.out.println("data source                                             : " + input);
		System.out.println("parallel source                                         : " + parallelSource);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("time characteristic 1-Processing 2-Event 3-Ingestion    : " + timeCharacteristic);
		System.out.println("pre-aggregate window [milliseconds]                     : " + preAggregationProcessingTimer);
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);

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

		DataStream<TaxiRide> rides = null;
		if (parallelSource) {
			rides = env.addSource(new TaxiRideSourceParallel(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		} else {
			rides = env.addSource(new TaxiRideSource(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		}

		DataStream<Tuple2<Long, Long>> tuples = rides.map(new TaxiRideDriverTokenizerMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		DataStream<Tuple2<Long, Long>> preAggregatedStream = null;
		if (!enableController && preAggregationProcessingTimer == -1) {
			// no combiner
			preAggregatedStream = tuples;
		} else if (!enableController && preAggregationProcessingTimer > 0) {
			// static combiner based on timeout
			PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
			preAggregatedStream = tuples.combine(taxiRidePreAggregateFunction, preAggregationProcessingTimer).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).disableChaining().slotSharingGroup(slotGroup01);
		} else if (enableController) {
			// dynamic combiner with PI controller
			PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
			preAggregatedStream = tuples.adCombine(taxiRidePreAggregateFunction, preAggregationProcessingTimer).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).disableChaining().slotSharingGroup(slotGroup01);
		}

		KeyedStream<Tuple2<Long, Long>, Long> keyedByDriverId = preAggregatedStream.keyBy(new TaxiRideKeySelector());

		DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId.reduce(new TaxiRideSumReduceFunction()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			rideCounts
				.map(new TaxiRideFlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			rideCounts
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else {
			System.out.println("discarding output");
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideCountPreAggregate.class.getSimpleName());
		// @formatter:on
	}
}
