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

package org.apache.flink.streaming.examples.partitioning;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.partitioning.util.WordSource;
import org.apache.flink.streaming.examples.partitioning.util.WordSourceType;
import org.apache.flink.util.Collector;

/**
 * <p>This example shows how to:
 * <ul>
 * <li>use different physical partition strategies.</li>
 * <li>This program count words using different partition strategies. The first strategy is the original hash partition from Flink. The second strategy is a load balance based on the frequency of the keys.</li>
 * </ul>
 * <p>
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition [original|partial] -window [true|false] -input [WORDS|FEW_WORDS|WORDS_SKEW|file] -poolingTimes [times] -output [] &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -input /home/felipe/Temp/t8.shakespeare.txt -window false &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -input /home/felipe/Temp/t8.shakespeare.txt -window true &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -input WORDS -window true &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -input FEW_WORDS -window true &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -input WORDS_SKEW -window true &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -skew-data-source false -window false -poolingTimes 1 &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -skew-data-source false -window true -poolingTimes 1 &
 * <p>
 */

@Public
public class WordCountKeyPartitioning {
	private static final String PARTITION = "partition";
	private static final String PARTITION_TYPE_PARTIAL = "partial";
	private static final String PARTITION_TYPE_ORIGINAL = "original";
	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String WINDOW = "window";
	private static final String POLLING_TIMES = "poolingTimes";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.disableOperatorChaining();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		String partitionStrategy = "";
		boolean window = false;
		long poolingTimes;

		String input = params.get(INPUT, WordSourceType.WORDS);
		partitionStrategy = params.get(PARTITION, PARTITION_TYPE_ORIGINAL);
		window = params.getBoolean(WINDOW, false);
		poolingTimes = params.getLong(POLLING_TIMES, Long.MAX_VALUE);

		// get input data
		DataStream<String> text;
		if (params.has(INPUT) && !WordSourceType.WORDS.equals(input) && !WordSourceType.WORDS_SKEW.equals(input) && !WordSourceType.FEW_WORDS.equals(input)) {
			// read the text file from given input path
			text = env.readTextFile(params.get(INPUT));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.addSource(new WordSource(input, poolingTimes)).name("source-" + partitionStrategy);
		}
		// Split string into tokens
		DataStream<Tuple2<String, Integer>> tokens = text.flatMap(new Tokenizer()).name("tokenizer-" + partitionStrategy);

		// choose a partitioning strategy
		KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = null;
		if (PARTITION_TYPE_ORIGINAL.equalsIgnoreCase(partitionStrategy)) {
			keyedStream = tokens.keyBy(0);
		} else if (PARTITION_TYPE_PARTIAL.equalsIgnoreCase(partitionStrategy)) {
			keyedStream = tokens.keyByPartial(0);
		}

		// Apply window or not -> sum -> print
		DataStream<Tuple2<String, Integer>> counts = null;
		if (window) {
			counts = keyedStream
				.window(TumblingEventTimeWindows.of(Time.seconds(3)))
				.sum(1).name("sum-" + partitionStrategy);
		} else {
			counts = keyedStream
				.sum(1).name("sum-" + partitionStrategy);
		}

		// emit result
		if (params.has(OUTPUT)) {
			counts.writeAsText(params.get("output")).name("print-" + partitionStrategy);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print().name("print-" + partitionStrategy);
		}

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute(WordCountKeyPartitioning.class.getSimpleName() + " strategy[" + partitionStrategy + "]");
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
}
