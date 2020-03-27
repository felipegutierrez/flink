/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateStrategy;
import org.apache.flink.streaming.examples.aggregate.util.DataRateSource;
import org.apache.flink.streaming.examples.aggregate.util.MqttDataSink;
import org.apache.flink.streaming.examples.aggregate.util.MqttDataSource;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;

/**
 * This is a dynamic pre-aggregator of items to be placed before the shuffle phase in a DAG. There are three types of use
 * case to test in this class.
 * First we test the DAG (word count example) without any pre-aggregator. Second, we test the pre-aggregator based only by
 * a time threshold. This is similar to a tumbling window. Finally, we test the pre-aggregator based on a time threshold
 * and on a max of items to aggregate. If the frequency of items is very high it is reasonable to shuffle data before the
 * time threshold to have timely results.
 *
 * <pre>
 * Changes the frequency that the pre-aggregate emits batches of data:
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "0"
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "10"
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "100"
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "1000"
 *
 * usage: java TopNPreAggregate \
 *        -pre-aggregate-window [>0 items] \
 *        -strategy [GLOBAL, LOCAL, PER_KEY] \
 *        -input mqtt \
 *        -sourceHost [127.0.0.1] -sourcePort [1883] \
 *        -controller [false] \
 *        -topN [long] \
 *        -pooling 100 \ # pooling frequency from source if not using mqtt data source
 *        -output [mqtt|log|text] \
 *        -sinkHost [127.0.0.1] -sinkPort [1883] \
 *        -slotSplit [false] -disableOperatorChaining [false]
 *
 * Running on the IDE:
 * usage: java TopNPreAggregate -pre-aggregate-window 1 -strategy GLOBAL -input mqtt -output mqtt
 *
 * </pre>
 */
public class TopNPreAggregate {

	private static final String TOPIC_DATA_SOURCE = "topic-data-source";
	private static final String TOPIC_DATA_SINK = "topic-data-sink";
	private static final String OPERATOR_SOURCE = "source";
	private static final String OPERATOR_SINK = "sink";
	private static final String OPERATOR_TOKENIZER = "tokenizer";
	private static final String OPERATOR_PRE_AGGREGATE = "pre-aggregate";
	private static final String OPERATOR_TOP_N = "topN";
	private static final String OPERATOR_FLAT_OUTPUT = "flat-output";
	private static final String SLOT_GROUP_LOCAL = "local-group";
	private static final String SLOT_GROUP_SHUFFLE = "shuffle-group";
	private static final String SLOT_GROUP_SPLIT = "slotSplit";
	private static final String DISABLE_OPERATOR_CHAINING = "disableOperatorChaining";
	private static final String CONTROLLER = "controller";

	private static final String PRE_AGGREGATE_WINDOW = "pre-aggregate-window";
	private static final String PRE_AGGREGATE_STRATEGY = "strategy";
	private static final String BUFFER_TIMEOUT = "bufferTimeout";
	private static final String TOP_N = "topN";
	private static final String POOLING_FREQUENCY = "pooling";
	private static final String SOURCE = "input";
	private static final String SOURCE_DATA_MQTT = "mqtt";
	private static final String SOURCE_HOST = "sourceHost";
	private static final String SOURCE_PORT = "sourcePort";
	private static final String SINK = "output";
	private static final String SINK_DATA_MQTT = "mqtt";
	private static final String SINK_HOST = "sinkHost";
	private static final String SINK_PORT = "sinkPort";
	private static final String SINK_LOG = "log";
	private static final String SINK_TEXT = "text";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		String input = params.get(SOURCE, "");
		String sourceHost = params.get(SOURCE_HOST, "127.0.0.1");
		int sourcePort = params.getInt(SOURCE_PORT, 1883);
		String output = params.get(SINK, "");
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		int poolingFrequency = params.getInt(POOLING_FREQUENCY, 0);
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 1);
		int topN = params.getInt(TOP_N, 10);
		long bufferTimeout = params.getLong(BUFFER_TIMEOUT, -999);
		boolean slotSplit = params.getBoolean(SLOT_GROUP_SPLIT, false);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);
		boolean controller = params.getBoolean(CONTROLLER, false);
		PreAggregateStrategy preAggregateStrategy = PreAggregateStrategy.valueOf(params.get(PRE_AGGREGATE_STRATEGY,
			PreAggregateStrategy.GLOBAL.toString()));

		String slotSharingGroup01 = null;
		String slotSharingGroup02 = null;
		if (slotSplit) {
			slotSharingGroup01 = SLOT_GROUP_LOCAL;
			slotSharingGroup02 = SLOT_GROUP_SHUFFLE;
		}

		System.out.println("data source                              : " + input);
		System.out.println("data source host:port                    : " + sourceHost + ":" + sourcePort);
		System.out.println("data source topic                        : " + TOPIC_DATA_SOURCE);
		System.out.println("data sink                                : " + output);
		System.out.println("data sink host:port                      : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                          : " + TOPIC_DATA_SINK);
		System.out.println("Feedback loop Controller                 : " + controller);
		System.out.println("Splitting into different slots           : " + slotSplit);
		System.out.println("Disable operator chaining                : " + disableOperatorChaining);
		System.out.println("pooling frequency [milliseconds]         : " + poolingFrequency);
		System.out.println("pre-aggregate window [count]             : " + preAggregationWindowCount);
		System.out.println("pre-aggregate strategy                   : " + preAggregateStrategy.getValue());
		System.out.println("topN                                     : " + topN);
		// System.out.println("pre-aggregate max items                  : " + maxToPreAggregate);
		System.out.println("BufferTimeout [milliseconds]             : " + bufferTimeout);
		System.out.println("Changing pooling frequency of the data source:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-data-source -m \"100\"");
		System.out.println("Changing pre-aggregation frequency before shuffling:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m \"100\"");

		if (bufferTimeout != -999) {
			env.setBufferTimeout(bufferTimeout);
		}
		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}

		// get input data
		DataStream<String> rawSensorValues;
		if (Strings.isNullOrEmpty(input)) {
			rawSensorValues = env.addSource(new DataRateSource(new String[0], poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_DATA_MQTT.equalsIgnoreCase(input)) {
			rawSensorValues = env.addSource(new MqttDataSource(TOPIC_DATA_SOURCE, sourceHost, sourcePort))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else {
			// read the text file from given input path
			rawSensorValues = env.readTextFile(params.get("input")).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		}

		// split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<Tuple2<Integer, Double>> sensorValues = rawSensorValues.flatMap(new SensorTokenizer())
			.name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotSharingGroup01);

		// Combine the stream
		PreAggregateFunction<Integer, Double[], Tuple2<Integer, Double>, Tuple2<Integer, Double>> topNPreAggregateFunction = new TopNPreAggregateFunction(topN);
		DataStream<Tuple2<Integer, Double>> preAggregatedStream = sensorValues
			.preAggregate(topNPreAggregateFunction, preAggregationWindowCount, preAggregateStrategy, controller)
			.name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotSharingGroup01);

		// group by the tuple field "0" and sum up tuple field "1"
		KeyedStream<Tuple2<Integer, Double>, Tuple> keyedStream = preAggregatedStream
			.keyBy(0);

		DataStream<Tuple2<Integer, Double>> resultStream = keyedStream
			.process(new TopNKeyedProcessFunction(topN)).name(OPERATOR_TOP_N).uid(OPERATOR_TOP_N).slotSharingGroup(slotSharingGroup02);

		// emit result
		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			resultStream
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotSharingGroup02)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		} else if (output.equalsIgnoreCase(SINK_LOG)) {
			resultStream.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			resultStream
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotSharingGroup02)
				.writeAsText(params.get("output")).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			resultStream
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotSharingGroup02)
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		}

		System.out.println("Execution plan >>>");
		System.err.println(env.getExecutionPlan());
		// execute program
		env.execute(TopNPreAggregate.class.getSimpleName());
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************
	public static final class SensorTokenizer implements FlatMapFunction<String, Tuple2<Integer, Double>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<Integer, Double>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\n");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					String[] sensorIdAndValue = token.split(";");
					if (sensorIdAndValue.length == 2) {
						out.collect(new Tuple2<Integer, Double>(Integer.valueOf(sensorIdAndValue[0]), Double.valueOf(sensorIdAndValue[1])));
					} else {
						System.out.println("WARNING: Sensor ID and VALUE do not match with pattern <Integer, Double>: " + sensorIdAndValue.toString());
					}
				}
			}
		}
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	private static class TopNPreAggregateFunction
		extends PreAggregateFunction<Integer, Double[], Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
		private final Double MIN_VALUE = -999999.9;
		private int topN;

		public TopNPreAggregateFunction(int topN) {
			this.topN = topN;
		}

		@Override
		public Double[] addInput(@Nullable Double[] value, Tuple2<Integer, Double> input) throws Exception {
			if (value == null) {
				value = new Double[this.topN];
				for (int i = 0; i < this.topN; i++) {
					if (i == this.topN - 1) {
						value[i] = input.f1;
					} else {
						value[i] = MIN_VALUE;
					}
				}
			} else {
				Arrays.sort(value);
				for (int i = this.topN - 1; i >= 0; i--) {
					if (value[i] < input.f1) {
						value[i] = input.f1;
						break;
					}
				}
			}
			return value;
		}

		@Override
		public void collect(Map<Integer, Double[]> buffer, Collector<Tuple2<Integer, Double>> out) throws Exception {
			for (Map.Entry<Integer, Double[]> entry : buffer.entrySet()) {
				Double[] values = entry.getValue();
				for (int i = 0; i < values.length; i++) {
					if (!values[i].equals(MIN_VALUE)) {
						out.collect(Tuple2.of(entry.getKey(), values[i]));
					}
				}
			}
		}

	}

	private static class TopNKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
		private final Double MIN_VALUE = -999999.9;
		private int topN;
		private ValueState<TopNValues> state;

		public TopNKeyedProcessFunction(int topN) {
			this.topN = topN;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", TopNValues.class));
		}

		@Override
		public void processElement(Tuple2<Integer, Double> value, Context ctx, Collector<Tuple2<Integer, Double>> out) throws Exception {
			TopNValues current = state.value();
			if (current == null) {
				current = new TopNValues(value.f0, value.f1, this.topN);
			} else {
				current.addValue(value.f1);
			}
			// set the state's timestamp to the record's assigned event time timestamp
			current.lastModified = ctx.timerService().currentProcessingTime();

			// write the state back
			state.update(current);

			// schedule the next timer 60 seconds from the current event time
			ctx.timerService().registerProcessingTimeTimer(current.lastModified + 30000);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Double>> out) throws Exception {
			TopNValues result = state.value();
			Double[] values = result.getValues();
			for (int i = 0; i < values.length; i++) {
				if (!values[i].equals(MIN_VALUE)) {
					out.collect(Tuple2.of(result.getSensorId(), values[i]));
				}
			}
		}
	}

	private static class TopNValues {
		private final Double MIN_VALUE = -999999.9;
		public long lastModified;
		private Integer sensorId;
		private Double[] values;

		public TopNValues(Integer sensorId, Double value, int topN) {
			this.sensorId = sensorId;
			this.values = new Double[topN];
			for (int i = 0; i < topN; i++) {
				if (i == topN - 1) {
					this.values[i] = value;
				} else {
					this.values[i] = MIN_VALUE;
				}
			}
		}

		public Integer getSensorId() {
			return this.sensorId;
		}

		public Double[] getValues() {
			return this.values;
		}

		public void addValue(Double newValue) {
			Arrays.sort(this.values);
			for (int i = this.values.length - 1; i >= 0; i--) {
				if (this.values[i] < newValue) {
					this.values[i] = newValue;
					break;
				}
			}
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<Integer, Double>, String> {
		@Override
		public String map(Tuple2<Integer, Double> value) throws Exception {
			return value.f0 + " [" + value + "]";
		}
	}
}
