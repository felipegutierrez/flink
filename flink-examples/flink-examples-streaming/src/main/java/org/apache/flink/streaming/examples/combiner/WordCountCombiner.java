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

package org.apache.flink.streaming.examples.combiner;

import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.CombineAdjustableFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.combiner.util.WordCountCombinerData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * usage: java WordCountCombiner -combiner dynamic -input skew
 */
public class WordCountCombiner {

	private static final String COMBINER = "combiner";
	private static final String COMBINER_STATIC = "static";
	private static final String COMBINER_DYNAMIC = "dynamic";
	private static final String SOURCE = "input";
	private static final String SOURCE_WORDS = "words";
	private static final String SOURCE_SKEW_WORDS = "skew";
	private static final String SOURCE_FEW_WORDS = "few";

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

		String combiner = params.get(COMBINER, "");
		String input = params.get(SOURCE, "");

		// get input data
		DataStream<String> text;

		if (Strings.isNullOrEmpty(input)) {
			text = env.fromElements(WordCountCombinerData.WORDS);
		} else if (SOURCE_WORDS.equalsIgnoreCase(input)) {
			text = env.fromElements(WordCountCombinerData.WORDS);
		} else if (SOURCE_SKEW_WORDS.equalsIgnoreCase(input)) {
			text = env.fromElements(WordCountCombinerData.SKEW_WORDS);
		} else if (SOURCE_FEW_WORDS.equalsIgnoreCase(input)) {
			text = env.fromElements(WordCountCombinerData.FEW_WORDS);
		} else {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		}

		// split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer());

		// Combine the stream
		DataStream<Tuple2<String, Integer>> combinedStream = null;
		CombineAdjustableFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> wordCountCombinerFunction = new CombinerWordCountImpl();

		if (Strings.isNullOrEmpty(combiner)) {
			combinedStream = counts; // no COMBINER
		} else if (COMBINER_STATIC.equalsIgnoreCase(combiner)) {
			combinedStream = counts.combine(wordCountCombinerFunction, 10); // STATIC COMBINER
		} else if (COMBINER_DYNAMIC.equalsIgnoreCase(combiner)) {
			combinedStream = counts.combine(wordCountCombinerFunction); // DYNAMIC COMBINER
		}

		// group by the tuple field "0" and sum up tuple field "1"
		DataStream<Tuple2<String, Integer>> resultStream = combinedStream
			.keyBy(0)
			.sum(1);

		// emit result
		if (params.has("output")) {
			resultStream.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			resultStream.print();
		}

		System.out.println("Execution plan >>>");
		System.err.println(env.getExecutionPlan());
		// execute program
		env.execute("Streaming WordCount");
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
	// GENERIC merge function to Static and Dynamic COMBINER's
	// *************************************************************************
	private static class CombinerWordCountImpl
		extends CombineAdjustableFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> {

		private static final Logger logger = LoggerFactory.getLogger(CombinerWordCountImpl.class);


		@Override
		public Integer addInput(@Nullable Integer value, Tuple2<String, Integer> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				return value + input.f1;
			}
		}

		@Override
		public void finishMerge(Map<String, Integer> buffer, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (Map.Entry<String, Integer> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}
}
