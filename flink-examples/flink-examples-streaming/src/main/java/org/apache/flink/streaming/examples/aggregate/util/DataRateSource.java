package org.apache.flink.streaming.examples.aggregate.util;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;

public class DataRateSource extends RichSourceFunction<String> {
	private volatile boolean running = true;
	private List<String> currentDataSource = null;

	public DataRateSource(String[] dataSource) {
		this.currentDataSource = Arrays.asList(dataSource);
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running) {
			for (String line : this.currentDataSource) {
				ctx.collect(line);
			}
			Thread.sleep(100);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
