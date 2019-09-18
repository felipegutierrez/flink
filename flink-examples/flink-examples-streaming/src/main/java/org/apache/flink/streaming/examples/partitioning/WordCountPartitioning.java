package org.apache.flink.streaming.examples.partitioning;

import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.partitioning.util.WordCountPartitionData;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Date;

/**
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition '' &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition rebalance &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition broadcast &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition shuffle &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition forward &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition rescale &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition global &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition custom &
 * ./bin/flink run examples/streaming/WordCountPartitioning.jar -partition partial &
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
	private static final String PARTITION_TYPE_CUSTOM = "custom";
	private static final String PARTITION_TYPE_PARTIAL = "partial";

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
		if (params.get(PARTITION) != null) {
			partitionStrategy = params.get(PARTITION);
		}

		// get input data
		DataStream<String> text = env.addSource(new WordSource(false, false)).name("source-" + partitionStrategy).uid("source-" + partitionStrategy);
		// DataStream<String> textSkew = env.addSource(new WordSource(false, true)).name("source-skew-" + partitionStrategy).uid("source-skew-" + partitionStrategy);

		// split lines in strings
		DataStream<Tuple2<String, Integer>> tokenizer = text
			.flatMap(new Tokenizer()).name("tokenizer-" + partitionStrategy).uid("tokenizer-" + partitionStrategy);
		// DataStream<Tuple2<String, Integer>> tokenizerSkew = textSkew
		// 	.flatMap(new Tokenizer()).name("tokenizer-skew-" + partitionStrategy).uid("tokenizer-skew-" + partitionStrategy);

		// choose a partitioning strategy
		DataStream<Tuple2<String, Integer>> partitionedStream = null;
		DataStream<Tuple2<String, Integer>> partitionedStreamSkew = null;
		if (!Strings.isNullOrEmpty(partitionStrategy)) {
			if (PARTITION_TYPE_REBALANCE.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.rebalance();
				// partitionedStreamSkew = tokenizerSkew.rebalance();
			} else if (PARTITION_TYPE_BROADCAST.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.broadcast();
				// partitionedStreamSkew = tokenizerSkew.broadcast();
			} else if (PARTITION_TYPE_SHUFFLE.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.shuffle();
				// partitionedStreamSkew = tokenizerSkew.shuffle();
			} else if (PARTITION_TYPE_FORWARD.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.forward();
				// partitionedStreamSkew = tokenizerSkew.forward();
			} else if (PARTITION_TYPE_RESCALE.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.rescale();
				// partitionedStreamSkew = tokenizerSkew.rescale();
			} else if (PARTITION_TYPE_GLOBAL.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.global();
				// partitionedStreamSkew = tokenizerSkew.global();
			} else if (PARTITION_TYPE_CUSTOM.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.partitionCustom(new WordPartitioner(), new WordKeySelector());
				// 	partitionedStreamSkew = tokenizerSkew.partitionCustom(new WordPartitioner(), new WordKeySelector());
			} else if (PARTITION_TYPE_PARTIAL.equalsIgnoreCase(partitionStrategy)) {
				partitionedStream = tokenizer.partitionByPartial(new WordKeySelector());
				// partitionedStreamSkew = tokenizerSkew.powerOfBothChoices(new WordKeySelector());
			} else {
				partitionedStream = tokenizer;
				// 	partitionedStreamSkew = tokenizerSkew;
			}
		} else {
			partitionedStream = tokenizer;
			// 	partitionedStreamSkew = tokenizerSkew;
		}

		partitionedStream
			.keyBy(0)
			.sum(1).name("sum-" + partitionStrategy).uid("sum-" + partitionStrategy)
			.print().name("print-" + partitionStrategy).uid("print-" + partitionStrategy);
		// DataStream<Tuple2<String, Integer>> replaceMapStream = partitionedStream.map(new WordReplaceMap()).name("map-" + partitionStrategy).uid("map-" + partitionStrategy);
		// DataStream<Tuple2<String, Integer>> replaceMapStreamSkew = partitionedStreamSkew.map(new WordReplaceMap()).name("map-skew-" + partitionStrategy).uid("map-skew-" + partitionStrategy);
		// process sum in a window using reduce
		// DataStream<Tuple2<String, Integer>> countStream =
		// replaceMapStream.union(replaceMapStreamSkew)
		// .keyBy(new WordKeySelector())
		// .window(TumblingEventTimeWindows.of(Time.seconds(5)))
		// .reduce(new SumWindowReduce()).name("reduce-" + partitionStrategy).uid("reduce-" + partitionStrategy)
		//.print().name("print-" + partitionStrategy).uid("print-" + partitionStrategy);

		// execute program
		System.out.println(env.getExecutionPlan());
		env.execute(WordCountPartitioning.class.getSimpleName() + " strategy[" + partitionStrategy + "]");
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
