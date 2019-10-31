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

import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * usage: java WordCountCombiner -pre-aggregate [static|dynamic|window-static|window-dynamic] -pre-aggregate-window [>0 seconds] -max-pre-aggregate [>=1 items] -input [hamlet|mobydick|dictionary|words|skew|few|variation] -window [>=0 seconds]
 *
 * Running on the IDE:
 * Running without a pre-aggregation
 * usage: java WordCountCombiner -pooling 100 -input dictionary -window 120
 * usage: java WordCountCombiner -pooling 100 -input hamlet -window 30
 * usage: java WordCountCombiner -pooling 100 -input hamlet -window 30
 * usage: java WordCountCombiner -pooling 100 -input variation -window 30
 *
 * Running with a static pre-aggregation every 10 seconds
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 10 -input dictionary -window 30
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 10 -input hamlet -window 30
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 10 -input mobydick -window 30
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 10 -input variation -window 30
 *
 * Running with a static pre-aggregation every 10 seconds or 10 items
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 10 -max-pre-aggregate 1000 -input dictionary -window 30
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 10 -max-pre-aggregate 1000 -input hamlet -window 30
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 10 -max-pre-aggregate 1000 -input mobydick -window 30
 * usage: java WordCountCombiner -pooling 100 -pre-aggregate-window 60 -max-pre-aggregate 200 -input variation -window 120
 *
 * Running on Standalone Flink cluster:
 * Running without a pre-aggregation
 * usage: ./bin/flink run WordCountPreAggregate.jar -input dictionary -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -input hamlet -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -input mobydick -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -input variation -window 30
 *
 * Running with a static pre-aggregation every 10 seconds
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -input dictionary -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -input hamlet -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -input mobydick -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -input variation -window 30
 *
 * Running with a static pre-aggregation every 10 seconds or 10 items
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -max-pre-aggregate 1000 -input dictionary -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -max-pre-aggregate 1000 -input hamlet -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -max-pre-aggregate 1000 -input mobydick -window 30
 * usage: ./bin/flink run WordCountPreAggregate.jar -pre-aggregate-window 10 -max-pre-aggregate 1000 -input variation -window 30
 * </pre>
 */
public class WordCountPreAggregate {

	private static final String OPERATOR_SOURCE = "source";
	private static final String OPERATOR_SINK = "sink";
	private static final String OPERATOR_TOKENIZER = "tokenizer";
	private static final String OPERATOR_SUM = "sum";

	private static final String WINDOW = "window";
	private static final String PRE_AGGREGATE_WINDOW = "pre-aggregate-window";
	private static final String MAX_PRE_AGGREGATE = "max-pre-aggregate";
	private static final String POOLING_FREQUENCY = "pooling";
	private static final String SOURCE = "input";
	private static final String SOURCE_WORDS = "words";
	private static final String SOURCE_SKEW_WORDS = "skew";
	private static final String SOURCE_FEW_WORDS = "few";
	private static final String SOURCE_DATA_RATE_VARIATION_WORDS = "variation";
	private static final String SOURCE_DATA_HAMLET = "hamlet";
	private static final String SOURCE_DATA_MOBY_DICK = "mobydick";
	private static final String SOURCE_DATA_DICTIONARY = "dictionary";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.disableOperatorChaining();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		String input = params.get(SOURCE, "");
		int window = params.getInt(WINDOW, 0);
		int poolingFrequency = params.getInt(POOLING_FREQUENCY, 1000);
		int preAggregationWindowTime = params.getInt(PRE_AGGREGATE_WINDOW, 0);
		int maxToPreAggregate = params.getInt(MAX_PRE_AGGREGATE, -1);

		System.out.println("data source                     : " + input);
		System.out.println("pooling frequency [milliseconds]: " + poolingFrequency);
		System.out.println("pre-aggregate window [seconds]  : " + preAggregationWindowTime);
		System.out.println("pre-aggregate max items         : " + maxToPreAggregate);
		System.out.println("window [seconds]                : " + window);

		// get input data
		DataStream<String> text;

		if (Strings.isNullOrEmpty(input)) {
			text = env.addSource(new DataRateSource(new String[0], poolingFrequency)).name(OPERATOR_SOURCE);
		} else if (SOURCE_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.WORDS, poolingFrequency)).name(OPERATOR_SOURCE);
		} else if (SOURCE_SKEW_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.SKEW_WORDS, poolingFrequency)).name(OPERATOR_SOURCE);
		} else if (SOURCE_FEW_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.FEW_WORDS, poolingFrequency)).name(OPERATOR_SOURCE);
		} else if (SOURCE_DATA_RATE_VARIATION_WORDS.equalsIgnoreCase(input)) {
			// creates a data rate variation to test how long takes to the dynamic combiner adapt
			text = env.addSource(new DataRateVariationSource(poolingFrequency)).name(OPERATOR_SOURCE);
		} else if (SOURCE_DATA_HAMLET.equalsIgnoreCase(input)) {
			text = env.addSource(new OnlineDataSource(UrlSource.HAMLET, poolingFrequency)).name(OPERATOR_SOURCE);
		} else if (SOURCE_DATA_MOBY_DICK.equalsIgnoreCase(input)) {
			text = env.addSource(new OnlineDataSource(UrlSource.MOBY_DICK, poolingFrequency)).name(OPERATOR_SOURCE);
		} else if (SOURCE_DATA_DICTIONARY.equalsIgnoreCase(input)) {
			text = env.addSource(new OnlineDataSource(UrlSource.ENGLISH_DICTIONARY, poolingFrequency)).name(OPERATOR_SOURCE);
		} else {
			// read the text file from given input path
			text = env.readTextFile(params.get("input")).name(OPERATOR_SOURCE);
		}

		// split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).name(OPERATOR_TOKENIZER);

		// Combine the stream
		DataStream<Tuple2<String, Integer>> preAggregatedStream = null;
		PreAggregateFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> wordCountPreAggregateFunction = new WordCountPreAggregateFunction();

		if (preAggregationWindowTime == 0) {
			// NO PRE_AGGREGATE
			preAggregatedStream = counts;
		} else if (preAggregationWindowTime > 0 && maxToPreAggregate == -1) {
			// STATIC PRE_AGGREGATE pre-aggregates every 10 seconds
			preAggregatedStream = counts.preAggregate(wordCountPreAggregateFunction, preAggregationWindowTime);
		} else if (preAggregationWindowTime > 0 && maxToPreAggregate > 0) {
			// DYNAMIC PRE_AGGREGATE pre-aggregates every 10 seconds or every 1000 items
			preAggregatedStream = counts.preAggregate(wordCountPreAggregateFunction, preAggregationWindowTime, maxToPreAggregate);
		}

		// group by the tuple field "0" and sum up tuple field "1"
		KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = preAggregatedStream
			.keyBy(0);

		DataStream<Tuple2<String, Integer>> resultStream = null;
		if (window != 0) {
			resultStream = keyedStream
				.window(TumblingProcessingTimeWindows.of(Time.seconds(window)))
				.sum(1).name(OPERATOR_SUM);
		} else {
			resultStream = keyedStream
				.sum(1).name(OPERATOR_SUM);
		}

		// emit result
		if (params.has("output")) {
			resultStream.writeAsText(params.get("output")).name(OPERATOR_SINK);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			resultStream.print().name(OPERATOR_SINK);
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

		@Override
		public Integer addInput(@Nullable Integer value, Tuple2<String, Integer> input) {
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
}
