package org.apache.flink.streaming.examples.partitioning;

import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.partitioning.util.WordCountPartitionData;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Date;

/**
 * ./bin/flink run examples/streaming/WordCountKeyPartitioning.jar -partition [original|partial] -skew-data-source [true|false] &
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use different physical partition strategies.
 * </ul>
 */

public class WordCountKeyPartitioning {
	private static final String PARTITION = "partition";
	private static final String PARTITION_TYPE_PARTIAL = "partial";
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
		if (params.get(PARTITION) != null) {
			partitionStrategy = params.get(PARTITION);
		}
		if (params.get(SKEW_DATA_SOURCE) != null) {
			skewDataSource = params.getBoolean(SKEW_DATA_SOURCE);
		}
		String name = "-" + partitionStrategy + "-" + skewDataSource;

		// choose a partitioning strategy
		KeyedStream<Tuple2<String, Integer>, Tuple> partition = null;
		if (!Strings.isNullOrEmpty(partitionStrategy)) {
			if (PARTITION_TYPE_ORIGINAL.equalsIgnoreCase(partitionStrategy)) {
				env.addSource(new WordSource(false, skewDataSource)).name("source" + name)
					.flatMap(new Tokenizer()).name("tokenizer" + name)
					.keyBy(0)
					.sum(1).name("sum" + name)
					.print().name("print" + partitionStrategy + "-" + skewDataSource);
			} else if (PARTITION_TYPE_PARTIAL.equalsIgnoreCase(partitionStrategy)) {
				env.addSource(new WordSource(false, skewDataSource)).name("source" + name)
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

	private static class WordReplaceMap implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
		@Override
		public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
			String v = value.f0
				.replace(":", "")
				.replace(".", "")
				.replace(",", "")
				.replace(";", "")
				.replace("-", "")
				.replace("_", "")
				.replace("\'", "")
				.replace("\"", "");
			return Tuple2.of(v, value.f1);
		}
	}

	private static class WordPartitioner implements Partitioner<String> {
		@Override
		public int partition(String key, int numPartitions) {
			return key.hashCode() % numPartitions;
		}
	}

	private static final class WordKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

	private static class SumWindowReduce implements ReduceFunction<Tuple2<String, Integer>> {
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
			return Tuple2.of(value1.f0, value1.f1 + value2.f1);
		}
	}

	private static class WordSource implements SourceFunction<String> {
		private static final long DEFAULT_INTERVAL_CHANGE_DATA_SOURCE = Time.minutes(1).toMilliseconds();
		private volatile boolean running = true;
		private long startTime;
		private boolean allowSkew;
		private boolean useDataSkewedFile;

		public WordSource(boolean allowSkew, boolean useDataSkewedFile) {
			this.allowSkew = allowSkew;
			this.useDataSkewedFile = useDataSkewedFile;
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
				Thread.sleep(3000);
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
}
