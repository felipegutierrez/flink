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

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateStrategy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.aggregate.util.*;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
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
 * usage: java WordCountPreAggregate \
 *        -pre-aggregate-window [>0 items] \
 *        -strategy [GLOBAL, LOCAL, PER_KEY] \
 *        -input [mqtt|hamlet|mobydick|dictionary|words|skew|few|variation] \
 *        -sourceHost [127.0.0.1] -sourcePort [1883] \
 *        -simulateSkew [false] \
 *        -controller [false] \
 *        -pooling 100 \ # pooling frequency from source if not using mqtt data source
 *        -output [mqtt|log|text] \
 *        -sinkHost [127.0.0.1] -sinkPort [1883] \
 *        -slotSplit [false] -disableOperatorChaining [false] \
 *        -window [>=0 seconds] \
 *        -latencyTrackingInterval [0]
 *
 * Running on the IDE:
 * usage: java WordCountPreAggregate [parameters]
 *
 * </pre>
 */
public class WordCountPreAggregate {

	private static final String TOPIC_DATA_SOURCE = "topic-data-source";
	private static final String TOPIC_DATA_SINK = "topic-data-sink";
	private static final String OPERATOR_SOURCE = "source";
	private static final String OPERATOR_SINK = "sink";
	private static final String OPERATOR_TOKENIZER = "tokenizer";
	private static final String OPERATOR_PRE_AGGREGATE = "pre-aggregate";
	private static final String OPERATOR_SUM = "sum";
	private static final String OPERATOR_FLAT_OUTPUT = "flat-output";
	private static final String SLOT_GROUP_LOCAL = "local-group";
	private static final String SLOT_GROUP_SHUFFLE = "shuffle-group";
	private static final String SLOT_GROUP_SPLIT = "slotSplit";
	private static final String DISABLE_OPERATOR_CHAINING = "disableOperatorChaining";
	private static final String LATENCY_TRACKING_INTERVAL = "latencyTrackingInterval";
	private static final String SIMULATE_SKEW = "simulateSkew";
	private static final String CONTROLLER = "controller";

	private static final String WINDOW = "window";
	private static final String PRE_AGGREGATE_WINDOW = "pre-aggregate-window";
	private static final String PRE_AGGREGATE_STRATEGY = "strategy";
	private static final String BUFFER_TIMEOUT = "bufferTimeout";
	private static final String SYNTHETIC_DELAY = "delay";
	private static final String POOLING_FREQUENCY = "pooling";
	private static final String SOURCE = "input";
	private static final String SOURCE_WORDS = "words";
	private static final String SOURCE_SKEW_WORDS = "skew";
	private static final String SOURCE_FEW_WORDS = "few";
	private static final String SOURCE_DATA_RATE_VARIATION_WORDS = "variation";
	private static final String SOURCE_DATA_HAMLET = "hamlet";
	private static final String SOURCE_DATA_MOBY_DICK = "mobydick";
	private static final String SOURCE_DATA_DICTIONARY = "dictionary";
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
		int window = params.getInt(WINDOW, 0);
		int poolingFrequency = params.getInt(POOLING_FREQUENCY, 0);
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 0);
		int controllerFrequencySec = params.getInt(CONTROLLER, -1);
		long bufferTimeout = params.getLong(BUFFER_TIMEOUT, -999);
		long delay = params.getLong(SYNTHETIC_DELAY, 0);
		long latencyTrackingInterval = params.getLong(LATENCY_TRACKING_INTERVAL, 0);
		boolean slotSplit = params.getBoolean(SLOT_GROUP_SPLIT, false);
		boolean simulateSkew = params.getBoolean(SIMULATE_SKEW, false);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);
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
		System.out.println("Simulating skew                          : " + simulateSkew);
		System.out.println("Feedback loop Controller                 : " + controllerFrequencySec);
		System.out.println("Splitting into different slots           : " + slotSplit);
		System.out.println("Disable operator chaining                : " + disableOperatorChaining);
		System.out.println("pooling frequency [milliseconds]         : " + poolingFrequency);
		System.out.println("pre-aggregate window [count]             : " + preAggregationWindowCount);
		System.out.println("pre-aggregate strategy                   : " + preAggregateStrategy.getValue());
		// System.out.println("pre-aggregate max items                  : " + maxToPreAggregate);
		System.out.println("window [seconds]                         : " + window);
		System.out.println("BufferTimeout [milliseconds]             : " + bufferTimeout);
		System.out.println("Synthetic delay [milliseconds]           : " + delay);
		System.out.println("Latency tracking interval [milliseconds] : " + latencyTrackingInterval);
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
		if (latencyTrackingInterval > 0) {
			env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);
		}

		// get input data
		DataStream<String> text;
		if (Strings.isNullOrEmpty(input)) {
			text = env.addSource(new DataRateSource(new String[0], poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.WORDS, poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_SKEW_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.SKEW_WORDS, poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_FEW_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.FEW_WORDS, poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_DATA_RATE_VARIATION_WORDS.equalsIgnoreCase(input)) {
			// creates a data rate variation to test how long takes to the dynamic combiner adapt
			text = env.addSource(new DataRateVariationSource(poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_DATA_HAMLET.equalsIgnoreCase(input)) {
			text = env.addSource(new OnlineDataSource(UrlSource.HAMLET, poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_DATA_MOBY_DICK.equalsIgnoreCase(input)) {
			text = env.addSource(new OnlineDataSource(UrlSource.MOBY_DICK, poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_DATA_DICTIONARY.equalsIgnoreCase(input)) {
			text = env.addSource(new OnlineDataSource(UrlSource.ENGLISH_DICTIONARY, poolingFrequency))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else if (SOURCE_DATA_MQTT.equalsIgnoreCase(input)) {
			text = env.addSource(new MqttDataSource(TOPIC_DATA_SOURCE, sourceHost, sourcePort))
				.name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		} else {
			// read the text file from given input path
			text = env.readTextFile(params.get("input")).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotSharingGroup01);
		}

		// split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
			.name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotSharingGroup01);

		DataStream<Tuple2<String, Integer>> skewed = null;
		if (simulateSkew) {
			skewed = counts.partitionCustom(new SimulateSkewPartition(), 0);
		} else {
			skewed = counts;
		}

		// Combine the stream
		DataStream<Tuple2<String, Integer>> preAggregatedStream = null;
		PreAggregateFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> wordCountPreAggregateFunction = new WordCountPreAggregateFunction(delay);

		if (preAggregationWindowCount == 0) {
			// NO PRE_AGGREGATE
			preAggregatedStream = skewed;
		} else {
			preAggregatedStream = skewed
				.preAggregate(wordCountPreAggregateFunction, preAggregationWindowCount, preAggregateStrategy, controllerFrequencySec)
				.name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotSharingGroup01);
		}

		// group by the tuple field "0" and sum up tuple field "1"
		KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = preAggregatedStream
			.keyBy(0);

		DataStream<Tuple2<String, Integer>> resultStream = null;
		if (window != 0) {
			resultStream = keyedStream
				.window(TumblingProcessingTimeWindows.of(Time.seconds(window)))
				.reduce(new SumReduceFunction(delay)).name(OPERATOR_SUM).uid(OPERATOR_SUM).slotSharingGroup(slotSharingGroup02);
			// .sum(1).name(OPERATOR_SUM);
		} else {
			resultStream = keyedStream
				.reduce(new SumReduceFunction(delay)).name(OPERATOR_SUM).uid(OPERATOR_SUM).slotSharingGroup(slotSharingGroup02);
			// .sum(1).name(OPERATOR_SUM);
		}

		// emit result
		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			resultStream
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotSharingGroup02)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		} else if (output.equalsIgnoreCase(SINK_LOG)) {
			resultStream.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			resultStream.writeAsText(params.get("output")).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			resultStream.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotSharingGroup02);
		}

		System.out.println("Execution plan >>>");
		System.err.println(env.getExecutionPlan());
		// execute program
		env.execute(WordCountPreAggregate.class.getSimpleName());
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	private static class WordCountPreAggregateFunction
		extends PreAggregateFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> {
		private long milliseconds = 0;

		public WordCountPreAggregateFunction(long milliseconds) {
			this.milliseconds = milliseconds;
		}

		@Override
		public Integer addInput(@Nullable Integer value, Tuple2<String, Integer> input) throws InterruptedException {
			Thread.sleep(milliseconds);
			if (value == null) {
				return input.f1;
			} else {
				return value + input.f1;
			}
		}

		@Override
		public void collect(Map<String, Integer> buffer, Collector<Tuple2<String, Integer>> out) {
			for (Map.Entry<String, Integer> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class SumReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
		private long milliseconds = 0;

		public SumReduceFunction(long milliseconds) {
			this.milliseconds = milliseconds;
		}

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
			Thread.sleep(milliseconds);
			return Tuple2.of(value1.f0, value1.f1 + value2.f1);
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<String, Integer>, String> {
		@Override
		public String map(Tuple2<String, Integer> value) throws Exception {
			return value.f0 + " - " + value.f1;
		}
	}

	private static class SimulateSkewPartition implements Partitioner<String> {
		private static final long serialVersionUID = 1L;
		private int nextChannelToSendTo;

		@Override
		public int partition(String key, int numPartitions) {
			if ("GUTENBERG".equalsIgnoreCase(key)) {
				return 0;
			} else {
				nextChannelToSendTo = (nextChannelToSendTo + 1) % numPartitions;
				return nextChannelToSendTo;
			}
		}
	}
}
