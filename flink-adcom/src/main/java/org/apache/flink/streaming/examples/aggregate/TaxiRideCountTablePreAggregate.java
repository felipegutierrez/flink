package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.MqttDataSink;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSource;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSourceParallel;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideTableOutputMap;
import org.apache.flink.streaming.examples.aggregate.util.ExerciseBase;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * <pre>
 * -mini_batch_enabled true -mini_batch_latency 1s -mini_batch_size 1000 -mini_batch_two_phase true -parallelism-table 4
 * </pre>
 */
public class TaxiRideCountTablePreAggregate {
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
		boolean mini_batch_enabled = params.getBoolean(TABLE_MINI_BATCH_ENABLE, false);
		int parallelismTableApi = params.getInt(TABLE_PARALLELISM, ExecutionConfig.PARALLELISM_DEFAULT);
		String mini_batch_allow_latency = params.get(TABLE_MINI_BATCH_LATENCY, "1s");
		String mini_batch_size = params.get(TABLE_MINI_BATCH_SIZE, "1000");
		boolean twoPhaseAgg = params.getBoolean(TABLE_MINI_BATCH_TWO_PHASE, false);

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
		System.out.println("Table API: mini-batch.enable                            : " + mini_batch_enabled);
		System.out.println("Table API: parallelism                                  : " + parallelismTableApi);
		System.out.println("Table API: mini-batch.latency                           : " + mini_batch_allow_latency);
		System.out.println("Table API: mini_batch.size                              : " + mini_batch_size);
		System.out.println("Table API: mini_batch.two_phase                         : " + twoPhaseAgg);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// access flink configuration
		Configuration configuration = tableEnv.getConfig().getConfiguration();
		// set low-level key-value options
		configuration.setInteger("table.exec.resource.default-parallelism", parallelismTableApi);
		// local-global aggregation depends on mini-batch is enabled
		configuration.setString("table.exec.mini-batch.enabled", Boolean.toString(mini_batch_enabled));
		configuration.setString("table.exec.mini-batch.allow-latency", mini_batch_allow_latency);
		configuration.setString("table.exec.mini-batch.size", mini_batch_size);
		// enable two-phase, i.e. local-global aggregation
		if (twoPhaseAgg) {
			configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
		}
		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}

		DataStream<TaxiRide> rides = null;
		if (parallelSource) {
			rides = env.addSource(new TaxiRideSourceParallel(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE);//.slotSharingGroup(slotGroup01);
		} else {
			rides = env.addSource(new TaxiRideSource(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE);//.slotSharingGroup(slotGroup01);
		}

		// "rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, passengerCnt, taxiId, driverId"
		Table ridesTableStream = tableEnv.fromDataStream(rides);

		Table resultTableStream = ridesTableStream
			.groupBy($("taxiId"))
			.select($("taxiId"), $("passengerCnt").count().as("passengerCnt"));

		// DataStream<TaxiRide> result = tableEnv.toAppendStream(resultTableStream, TaxiRide.class);
		TypeInformation<Tuple2<Long, Long>> typeInfo = TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
		});
		tableEnv
			.toRetractStream(resultTableStream, typeInfo)
			.map(new TaxiRideTableOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT)
			.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK);

		System.out.println(env.getExecutionPlan());
		env.execute(TaxiRideCountTablePreAggregate.class.getSimpleName());

		/*
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
		 */
		// @formatter:on
	}
}
