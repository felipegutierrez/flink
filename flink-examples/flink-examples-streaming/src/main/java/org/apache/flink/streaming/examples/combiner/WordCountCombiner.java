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

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.IFrequency;
import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamCombinerDynamicOperator;
import org.apache.flink.streaming.api.operators.StreamCombinerOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.combiner.util.WordCountCombinerData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Calendar;
import java.util.Map;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountCombinerData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WordCountCombiner {

	private static final String COMBINER = "combiner";
	private static final String COMBINER_STATIC = "static";
	private static final String COMBINER_DYNAMIC = "dynamic";

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

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			// text = env.fromElements(WordCountCombinerData.WORDS);
			text = env.fromElements(WordCountCombinerData.WORDS_SKEW);
		}

		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new Tokenizer());

		// Combine the stream
		DataStream<Tuple2<String, Integer>> combinedStream = null;
		KeySelector<Tuple2<String, Integer>, String> keyBundleSelector = (KeySelector<Tuple2<String, Integer>, String>) value -> value.f0;
		CombinerFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> wordCountCombinerFunction = new CombinerWordCountImpl();
		TypeInformation<Tuple2<String, Integer>> outTypeInfo = TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
		});

		if (Strings.isNullOrEmpty(combiner)) {
			combinedStream = counts;
		} else if (COMBINER_STATIC.equalsIgnoreCase(combiner)) {
			// static combiner
			CountCombinerTrigger<Tuple2<String, Integer>> bundleTrigger = new CountCombinerTrigger<Tuple2<String, Integer>>(10, 5);
			combinedStream = counts.combine(outTypeInfo, new StreamCombinerOperator<>(wordCountCombinerFunction, bundleTrigger, keyBundleSelector));
		} else if (COMBINER_DYNAMIC.equalsIgnoreCase(combiner)) {
			// static combiner
			CountCombinerTriggerDynamic<String, Tuple2<String, Integer>> bundleTrigger = new CountCombinerTriggerDynamic<String, Tuple2<String, Integer>>(5);
			combinedStream = counts.combine(outTypeInfo, new StreamCombinerDynamicOperator<>(wordCountCombinerFunction, bundleTrigger, keyBundleSelector));
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

		System.out.println("Execution plan:");
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
	public static final class CombinerWordCountImpl extends CombinerFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> {

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
		public void finishBundle(Map<String, Integer> buffer, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (Map.Entry<String, Integer> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	// *************************************************************************
	// STATIC COMBINER trigger
	// *************************************************************************
	public static final class CountCombinerTrigger<T> implements CombinerTrigger<T> {
		private static final Logger logger = LoggerFactory.getLogger(CountCombinerTrigger.class);

		private final long maxCount;
		private transient CombinerTriggerCallback callback;
		private transient long count = 0;
		private transient long startTime;
		private transient long timeout;

		public CountCombinerTrigger(long maxCount, long timeout) {
			Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
			this.maxCount = maxCount;
			this.timeout = timeout;
			this.startTime = Calendar.getInstance().getTimeInMillis();
		}

		@Override
		public void registerCallback(CombinerTriggerCallback callback) {
			this.callback = Preconditions.checkNotNull(callback, "callback is null");
			this.startTime = Calendar.getInstance().getTimeInMillis();
		}

		@Override
		public void onElement(T element) throws Exception {
			count++;
			long beforeTime = Calendar.getInstance().getTimeInMillis() - Time.seconds(timeout).toMilliseconds();
			if (count >= maxCount || beforeTime >= startTime) {
				callback.finishBundle();
				reset();
			}
		}

		@Override
		public void reset() {
			count = 0;
		}

		@Override
		public String explain() {
			return "CountBundleTrigger with size " + maxCount;
		}
	}

	// *************************************************************************
	// DYNAMIC COMBINER trigger
	// *************************************************************************
	public static final class CountCombinerTriggerDynamic<K, T> implements CombinerTriggerDynamic<K, T> {
		private static final Logger logger = LoggerFactory.getLogger(CountCombinerTriggerDynamic.class);

		private final long LIMIT_MIN_COUNT = 1;
		private final long INCREMENT = 10;

		private long maxCount;
		private transient long count = 0;
		private transient long timeout;
		private transient CombinerTriggerCallback callback;
		private transient IFrequency frequency;
		private transient long maxFrequencyCMS = 0;
		private transient long startTime;

		public CountCombinerTriggerDynamic() throws Exception {
			this(20);
		}

		public CountCombinerTriggerDynamic(long timeout) throws Exception {
			initFrequencySketch();
			this.maxCount = LIMIT_MIN_COUNT;
			this.startTime = Calendar.getInstance().getTimeInMillis();
			this.timeout = timeout;
			Preconditions.checkArgument(this.maxCount > 0, "maxCount must be greater than 0");
		}

		@Override
		public void registerCallback(CombinerTriggerCallback callback) {
			this.callback = Preconditions.checkNotNull(callback, "callback is null");
			this.startTime = Calendar.getInstance().getTimeInMillis();
			initFrequencySketch();
		}

		/**
		 * The Combiner is triggered when the count reaches the maxCount or by a timeout
		 */
		@Override
		public void onElement(K key, T element) throws Exception {
			// add key element on the HyperLogLog to infer the data-stream cardinality
			this.frequency.add(key.toString(), 1);
			long itemCMS = this.frequency.estimateCount(key.toString());
			if (itemCMS > this.maxFrequencyCMS) {
				this.maxFrequencyCMS = itemCMS;
			}
			count++;
			long beforeTime = Calendar.getInstance().getTimeInMillis() - Time.seconds(timeout).toMilliseconds();
			if (count >= maxCount || beforeTime >= startTime) {
				callback.finishBundle();
			}
		}

		@Override
		public void reset() throws Exception {
			if (count != 0) {
				String msg = "Thread[" + Thread.currentThread().getId() + "] frequencyCMS[" + maxFrequencyCMS + "] maxCount[" + maxCount + "]";
				if (maxFrequencyCMS > maxCount + INCREMENT) {
					// It is necessary to increase the combiner
					long diff = maxFrequencyCMS - maxCount;
					maxCount = maxCount + diff;
					msg = msg + " - INCREASING >>>";
					resetFrequencySketch();
				} else if (maxFrequencyCMS < maxCount - INCREMENT) {
					// It is necessary to reduce the combiner
					maxCount = maxFrequencyCMS + INCREMENT;
					msg = msg + " - DECREASING <<<";
					resetFrequencySketch();
				} else {
					msg = msg + " - HOLDING";
					this.startTime = Calendar.getInstance().getTimeInMillis();
				}
				System.out.println(msg);
				// logger.info(msg);
			}
			count = 0;
		}

		private void initFrequencySketch() {
			if (this.frequency == null) {
				this.frequency = new CountMinSketch(10, 5, 0);
			}
			this.maxFrequencyCMS = 0;
			this.startTime = Calendar.getInstance().getTimeInMillis();
		}

		private void resetFrequencySketch() {
			this.frequency = new CountMinSketch(10, 5, 0);
			this.maxFrequencyCMS = 0;
			this.startTime = Calendar.getInstance().getTimeInMillis();
		}

		@Override
		public String explain() {
			return "CountCombinerTriggerDynamic with size " + maxCount;
		}
	}
}
