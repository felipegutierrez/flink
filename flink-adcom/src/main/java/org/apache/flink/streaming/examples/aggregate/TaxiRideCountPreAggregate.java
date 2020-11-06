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
import org.apache.flink.streaming.examples.aggregate.util.GenericParameters;
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
		GenericParameters genericParam = new GenericParameters(args);
		genericParam.printParameters();

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (genericParam.isDisableOperatorChaining()) {
			env.disableOperatorChaining();
		}
		String slotGroup01 = SLOT_GROUP_DEFAULT;
		String slotGroup02 = SLOT_GROUP_DEFAULT;
		if (genericParam.getSlotSplit() == 0) {
			slotGroup01 = SLOT_GROUP_DEFAULT;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (genericParam.getSlotSplit() == 1) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (genericParam.getSlotSplit() == 2) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_01_02;
		}

		DataStream<TaxiRide> rides = null;
		if (genericParam.isParallelSource()) {
			rides = env.addSource(new TaxiRideSourceParallel(genericParam.getInput())).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		} else {
			rides = env.addSource(new TaxiRideSource(genericParam.getInput())).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		}

		DataStream<Tuple2<Long, Long>> tuples = rides.map(new TaxiRideDriverTokenizerMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		DataStream<Tuple2<Long, Long>> preAggregatedStream = null;
		if (!genericParam.isEnableController() && genericParam.getPreAggregationProcessingTimer() == -1) {
			// no combiner
			preAggregatedStream = tuples;
		} else if (!genericParam.isEnableController() && genericParam.getPreAggregationProcessingTimer() > 0) {
			// static combiner based on timeout
			PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
			preAggregatedStream = tuples.combine(taxiRidePreAggregateFunction, genericParam.getPreAggregationProcessingTimer()).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).disableChaining().slotSharingGroup(slotGroup01);
		} else if (genericParam.isEnableController()) {
			// dynamic combiner with PI controller
			PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> taxiRidePreAggregateFunction = new TaxiRideCountPreAggregateFunction();
			preAggregatedStream = tuples.adCombine(taxiRidePreAggregateFunction, genericParam.getPreAggregationProcessingTimer()).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).disableChaining().slotSharingGroup(slotGroup01);
		}

		KeyedStream<Tuple2<Long, Long>, Long> keyedByDriverId = preAggregatedStream.keyBy(new TaxiRideKeySelector());

		DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId.reduce(new TaxiRideSumReduceFunction()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());

		if (genericParam.getOutput().equalsIgnoreCase(SINK_DATA_MQTT)) {
			rideCounts
				.map(new TaxiRideFlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02())
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, genericParam.getSinkHost(), genericParam.getSinkPort())).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else if (genericParam.getOutput().equalsIgnoreCase(SINK_TEXT)) {
			rideCounts
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else {
			System.out.println("discarding output");
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideCountPreAggregate.class.getSimpleName());
		// @formatter:on
	}
}
