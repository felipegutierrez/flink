package org.apache.flink.streaming.examples.partitioning.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Calendar;
import java.util.Date;

public class WordSource implements SourceFunction<String> {
	private static final long DEFAULT_INTERVAL_CHANGE_DATA_SOURCE = Time.minutes(1).toMilliseconds();
	private volatile boolean running = true;
	private long startTime;
	private boolean allowSkew;
	private boolean useDataSkewedFile;
	private long times;

	public WordSource(boolean allowSkew, boolean useDataSkewedFile) {
		this(allowSkew, useDataSkewedFile, -1);
	}

	public WordSource(boolean allowSkew, boolean useDataSkewedFile, long times) {
		this.allowSkew = allowSkew;
		this.useDataSkewedFile = useDataSkewedFile;
		this.startTime = Calendar.getInstance().getTimeInMillis();
		this.times = times;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		long count = 0;
		while (running && count < times) {
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
			count++;
		}
		cancel();
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
