package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
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
import java.util.Random;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

public class TaxiRidePassengerAveragePreAggregate {
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, ExerciseBase.pathToRideData);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, "");
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 0);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);
		PreAggregateStrategy preAggregateStrategy = PreAggregateStrategy.valueOf(params.get(PRE_AGGREGATE_STRATEGY,
			PreAggregateStrategy.GLOBAL.toString()));

		System.out.println("Download data from:");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiRides.gz");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiFares.gz");
		System.out.println("data source                                             : " + input);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("pre-aggregate window [count]                            : " + preAggregationWindowCount);
		System.out.println("pre-aggregate strategy                                  : " + preAggregateStrategy.getValue());
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
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

		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		DataStream<Tuple2<Integer, Double>> tuples = rides.map(new TokenizerMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		PreAggregateFunction<Integer, Tuple2<Integer, Tuple2<Double, Long>>, Tuple2<Integer, Double>,
			Tuple2<Integer, Tuple2<Double, Long>>> taxiRidePreAggregateFunction = new TaxiRidePassengerSumPreAggregateFunction();
		DataStream<Tuple2<Integer, Tuple2<Double, Long>>> preAggregatedStream = tuples
			.combiner(taxiRidePreAggregateFunction, preAggregationWindowCount, enableController, preAggregateStrategy)
			.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);

		KeyedStream<Tuple2<Integer, Tuple2<Double, Long>>, Integer> keyedByRandomDriver = preAggregatedStream.keyBy(new RandomDriverKeySelector());

		DataStream<Tuple2<Integer, Tuple2<Double, Long>>> averagePassengers = keyedByRandomDriver
			.reduce(new AveragePassengersReducer()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			averagePassengers
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else {
			averagePassengers
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRidePassengerAveragePreAggregate.class.getSimpleName());
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	private static class TokenizerMap implements MapFunction<TaxiRide, Tuple2<Integer, Double>> {
		private final Random random;

		public TokenizerMap() {
			random = new Random();
		}

		@Override
		public Tuple2<Integer, Double> map(TaxiRide ride) {
			// create random keys from 0 to 10 in order to average the passengers of all taxi drivers
			int low = 0;
			int high = 10;
			Integer result = random.nextInt(high - low) + low;
			return Tuple2.of(result, Double.valueOf(ride.passengerCnt));
		}
	}

	/**
	 * Count the number of values and sum them.
	 * Key (Integer): random-key
	 * Value (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
	 * Input (Integer, Double): random-key, passengerCnt
	 * Output (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
	 */
	private static class TaxiRidePassengerSumPreAggregateFunction
		extends PreAggregateFunction<Integer,
		Tuple2<Integer, Tuple2<Double, Long>>,
		Tuple2<Integer, Double>,
		Tuple2<Integer, Tuple2<Double, Long>>> {

		@Override
		public Tuple2<Integer, Tuple2<Double, Long>> addInput(@Nullable Tuple2<Integer, Tuple2<Double, Long>> value, Tuple2<Integer, Double> input) throws Exception {
			Integer randomKey = input.f0;
			if (value == null) {
				Double passengersSum = input.f1;
				return Tuple2.of(randomKey, Tuple2.of(passengersSum, 1L));
			} else {
				Double passengersSum = input.f1 + value.f1.f0;
				Long driverIdCount = value.f1.f1 + 1;
				return Tuple2.of(randomKey, Tuple2.of(passengersSum, driverIdCount));
			}
		}

		@Override
		public void collect(Map<Integer, Tuple2<Integer, Tuple2<Double, Long>>> buffer, Collector<Tuple2<Integer, Tuple2<Double, Long>>> out) throws Exception {
			for (Map.Entry<Integer, Tuple2<Integer, Tuple2<Double, Long>>> entry : buffer.entrySet()) {
				Double passengerSum = entry.getValue().f1.f0;
				Long driverIdCount = entry.getValue().f1.f1;
				out.collect(Tuple2.of(entry.getKey(), Tuple2.of(passengerSum, driverIdCount)));
			}
		}
	}

	private static class RandomDriverKeySelector implements KeySelector<Tuple2<Integer, Tuple2<Double, Long>>, Integer> {
		@Override
		public Integer getKey(Tuple2<Integer, Tuple2<Double, Long>> value) throws Exception {
			return value.f0;
		}
	}

	private static class AveragePassengersReducer implements ReduceFunction<Tuple2<Integer, Tuple2<Double, Long>>> {
		@Override
		public Tuple2<Integer, Tuple2<Double, Long>> reduce(Tuple2<Integer, Tuple2<Double, Long>> value1,
															Tuple2<Integer, Tuple2<Double, Long>> value2) throws Exception {
			Integer randomKey = value1.f0;
			Double sum = value1.f1.f0 + value2.f1.f0;
			Long count = value1.f1.f1 + value2.f1.f1;
			Double avg = sum / count;
			return Tuple2.of(randomKey, Tuple2.of(avg, 1L));
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<Integer, Tuple2<Double, Long>>, String> {
		@Override
		public String map(Tuple2<Integer, Tuple2<Double, Long>> value) {
			return "Average passengers[" + value.f1.f0 + "]";
		}
	}
}
