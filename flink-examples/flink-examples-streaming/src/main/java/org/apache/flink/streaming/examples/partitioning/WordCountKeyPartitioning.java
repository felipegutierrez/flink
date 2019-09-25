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

import com.google.common.base.Strings;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.partitioning.util.WordSource;
import org.apache.flink.util.Collector;

/**
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition [original|partial] -skew-data-source [true|false] -poolingTimes -1 &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition original -skew-data-source false -poolingTimes 1 &
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition partial -skew-data-source false -poolingTimes 1 &
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use different physical partition strategies.
 * </ul>
 */

@Public
public class WordCountKeyPartitioning {
	private static final String PARTITION = "partition";
	private static final String PARTITION_TYPE_PARTIAL = "partial";
	private static final String POLLING_TIMES = "poolingTimes";
	private static final String PARTITION_TYPE_ORIGINAL = "original";
	private static final String SKEW_DATA_SOURCE = "skew-data-source";

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
		boolean skewDataSource = false;
		long poolingTimes;
		if (params.get(PARTITION) != null) {
			partitionStrategy = params.get(PARTITION);
		}
		if (params.get(SKEW_DATA_SOURCE) != null) {
			skewDataSource = params.getBoolean(SKEW_DATA_SOURCE);
		}
		poolingTimes = params.getLong(POLLING_TIMES, -1);
		String name = "-" + partitionStrategy + "-" + skewDataSource;

		// choose a partitioning strategy
		if (!Strings.isNullOrEmpty(partitionStrategy)) {
			if (PARTITION_TYPE_ORIGINAL.equalsIgnoreCase(partitionStrategy)) {
				env.addSource(new WordSource(false, skewDataSource, poolingTimes)).name("source" + name)
					.flatMap(new Tokenizer()).name("tokenizer" + name)
					.keyBy(0)
					.sum(1).name("sum" + name)
					.print().name("print" + partitionStrategy + "-" + skewDataSource);
			} else if (PARTITION_TYPE_PARTIAL.equalsIgnoreCase(partitionStrategy)) {
				env.addSource(new WordSource(false, skewDataSource, poolingTimes)).name("source" + name)
					.flatMap(new Tokenizer()).name("tokenizer" + name)
					.keyByPartial(0)
					.sum(1).name("sum" + name)
					.print().name("print" + name);
			}
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
