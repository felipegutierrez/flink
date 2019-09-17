package org.apache.flink.streaming.examples.partitioning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.partitioning.util.WordCountPartitionData;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Date;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */

public class WordCountPartitioning {
	private static final String PARTITION = "partition";
	private static final String PARTITION_TYPE_REBALANCE = "rebalance";
	private static final String PARTITION_TYPE_BROADCAST = "broadcast";
	private static final String PARTITION_TYPE_SHUFFLE = "shuffle";
	private static final String PARTITION_TYPE_FORWARD = "forward";
	private static final String PARTITION_TYPE_RESCALE = "rescale";
	private static final String PARTITION_TYPE_GLOBAL = "global";
	private static final String PARTITION_TYPE_POWER_OF_BOTH_CHOICES = "power";

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

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			// text = env.fromElements(WordCountPartitionData.WORDS);
			text = env.addSource(new WordSource(true)).name("source").uid("source");
		}

		DataStream<Tuple2<String, Integer>> tokenizer = text
			.flatMap(new Tokenizer()).name("tokenizer").uid("tokenizer");

		DataStream<Tuple2<String, Integer>> partitioning = null;
		if (params.get(PARTITION) != null) {
			if (params.get(PARTITION).equalsIgnoreCase(PARTITION_TYPE_REBALANCE)) {
				partitioning = tokenizer.rebalance();
			} else if (params.get(PARTITION).equalsIgnoreCase(PARTITION_TYPE_BROADCAST)) {
				partitioning = tokenizer.broadcast();
			} else if (params.get(PARTITION).equalsIgnoreCase(PARTITION_TYPE_SHUFFLE)) {
				partitioning = tokenizer.shuffle();
			} else if (params.get(PARTITION).equalsIgnoreCase(PARTITION_TYPE_FORWARD)) {
				partitioning = tokenizer.forward();
			} else if (params.get(PARTITION).equalsIgnoreCase(PARTITION_TYPE_RESCALE)) {
				partitioning = tokenizer.rescale();
			} else if (params.get(PARTITION).equalsIgnoreCase(PARTITION_TYPE_GLOBAL)) {
				partitioning = tokenizer.global();
			} else if (params.get(PARTITION).equalsIgnoreCase(PARTITION_TYPE_POWER_OF_BOTH_CHOICES)) {
				partitioning = tokenizer.powerOfBothChoices(new WordKeySelector());
			} else {
				System.err.println("Wrong partition strategy chosen.");
				partitioning = tokenizer;
			}
		} else {
			partitioning = tokenizer;
		}
		DataStream<Tuple2<String, Integer>> counts = partitioning
			.keyBy(0)
			.window(TumblingEventTimeWindows.of(Time.seconds(5)))
			.sum(1).name("sum").uid("sum");

		// emit result
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print().name("print").uid("print");
		}

		// execute program
		env.execute("Streaming WordCount with partition strategy");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private static class WordSource implements SourceFunction<String> {
		private static final long DEFAULT_INTERVAL_CHANGE_DATA_SOURCE = Time.minutes(1).toMilliseconds();
		private volatile boolean running = true;
		private long startTime;
		private boolean allowSkew;
		private boolean useDataSkewedFile;

		public WordSource(boolean allowSkew) {
			this.allowSkew = allowSkew;
			this.startTime = Calendar.getInstance().getTimeInMillis();
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running) {
				final String[] words;
				if (useDataSkewedFile()) {
					words = WordCountPartitionData.WORDS_SKEW;
				} else {
					words = WordCountPartitionData.WORDS;
				}
				for (int i = 0; i < words.length; i++) {
					String word = words[i];
					ctx.collectWithTimestamp(word, (new Date()).getTime());
				}
				Thread.sleep(4000);
			}
		}

		private boolean useDataSkewedFile() {
			if (allowSkew) {
				long elapsedTime = Calendar.getInstance().getTimeInMillis() - DEFAULT_INTERVAL_CHANGE_DATA_SOURCE;
				if (elapsedTime >= startTime) {
					startTime = Calendar.getInstance().getTimeInMillis();
					useDataSkewedFile = (!useDataSkewedFile);

					String msg = "Changed source file. useDataSkewedFile[" + useDataSkewedFile + "]";
					System.out.println(msg);
				}
			}
			return useDataSkewedFile;
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

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

	private static final class WordKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}
}
