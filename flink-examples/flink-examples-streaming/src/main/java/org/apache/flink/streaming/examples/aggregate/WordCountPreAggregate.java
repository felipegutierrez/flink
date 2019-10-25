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
import org.apache.flink.streaming.examples.aggregate.util.DataRateSource;
import org.apache.flink.streaming.examples.aggregate.util.DataRateVariationSource;
import org.apache.flink.streaming.examples.aggregate.util.WordCountPreAggregateData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * <pre>
 * usage: java WordCountCombiner -pre-aggregate static -input skew -window 30
 * usage: java WordCountCombiner -pre-aggregate static -input variation
 * usage: ./bin/flink run WordCountCombiner.jar -combiner dynamic -input variation
 * </pre>
 */
public class WordCountPreAggregate {

	private static final String OPERATOR_SOURCE = "source";
	private static final String OPERATOR_SINK = "sink";
	private static final String OPERATOR_TOKENIZER = "tokenizer";
	private static final String OPERATOR_SUM = "sum";

	private static final String PRE_AGGREGATE = "pre-aggregate";
	private static final String PRE_AGGREGATE_STATIC = "static";
	private static final String PRE_AGGREGATE_DYNAMIC = "dynamic";
	private static final String WINDOW = "window";
	private static final String SOURCE = "input";
	private static final String SOURCE_WORDS = "words";
	private static final String SOURCE_SKEW_WORDS = "skew";
	private static final String SOURCE_FEW_WORDS = "few";
	private static final String SOURCE_DATA_RATE_VARIATION_WORDS = "variation";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		String preAggregate = params.get(PRE_AGGREGATE, "");
		String input = params.get(SOURCE, "");
		int window = params.getInt(WINDOW, 0);

		// get input data
		DataStream<String> text;

		if (Strings.isNullOrEmpty(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.WORDS)).name(OPERATOR_SOURCE);
		} else if (SOURCE_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.WORDS)).name(OPERATOR_SOURCE);
		} else if (SOURCE_SKEW_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.SKEW_WORDS)).name(OPERATOR_SOURCE);
		} else if (SOURCE_FEW_WORDS.equalsIgnoreCase(input)) {
			text = env.addSource(new DataRateSource(WordCountPreAggregateData.FEW_WORDS)).name(OPERATOR_SOURCE);
		} else if (SOURCE_DATA_RATE_VARIATION_WORDS.equalsIgnoreCase(input)) {
			// creates a data rate variation to test how long takes to the dynamic combiner adapt
			text = env.addSource(new DataRateVariationSource()).name(OPERATOR_SOURCE);
		} else {
			// read the text file from given input path
			text = env.readTextFile(params.get("input")).name(OPERATOR_SOURCE);
		}

		// split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).name(OPERATOR_TOKENIZER);

		// Combine the stream
		DataStream<Tuple2<String, Integer>> preAggregatedStream = null;
		PreAggregateFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> wordCountPreAggregateFunction = new WordCountPreAggregateFunction();

		if (Strings.isNullOrEmpty(preAggregate)) {
			// NO PRE_AGGREGATE
			preAggregatedStream = counts;
		} else if (PRE_AGGREGATE_STATIC.equalsIgnoreCase(preAggregate)) {
			// STATIC PRE_AGGREGATE combines every 10 words or on the timeout
			preAggregatedStream = counts.preAggregate(wordCountPreAggregateFunction, 10);
		} else if (PRE_AGGREGATE_DYNAMIC.equalsIgnoreCase(preAggregate)) {
			// DYNAMIC PRE_AGGREGATE combines according the frequency of words or on the timeout
			preAggregatedStream = counts.preAggregateDynamic(wordCountPreAggregateFunction, 10);
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

		private static final Logger logger = LoggerFactory.getLogger(WordCountPreAggregateFunction.class);


		@Override
		public Integer addInput(@Nullable Integer value, Tuple2<String, Integer> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				return value + input.f1;
			}
		}

		@Override
		public void collect(Map<String, Integer> buffer, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (Map.Entry<String, Integer> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}
}
