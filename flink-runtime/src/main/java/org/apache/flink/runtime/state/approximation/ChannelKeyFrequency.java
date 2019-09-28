package org.apache.flink.runtime.state.approximation;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.IFrequency;

public class ChannelKeyFrequency {

	private final int INDEX_CHANNEL = 0;
	private final int INDEX_KEY_HIGHEST_FREQUENCY = 1;

	private IFrequency frequency;
	private long[][] channelState;
	private long factor;

	public ChannelKeyFrequency(int parallelism, long factor) {
		this(parallelism, factor, 12, 2045, 1);
	}

	public ChannelKeyFrequency(int parallelism, long factor, int depth, int width, int seed) {
		this.frequency = new CountMinSketch(depth, width, seed);
		this.channelState = new long[parallelism][2];
		this.factor = factor;
	}

	public long getChannelFrequencyState(int position) {
		return this.channelState[position][INDEX_KEY_HIGHEST_FREQUENCY];
	}

	public void add(Object key, int channel) {
		this.frequency.add(key.toString(), 1);
		long keyFrequencyEstimation = estimateCount(key);
		addKeyChannelState(channel, keyFrequencyEstimation);
	}

	public void add(long key, int channel) {
		this.frequency.add(key, 1);
		long keyFrequencyEstimation = estimateCount(key);
		addKeyChannelState(channel, keyFrequencyEstimation);
	}

	public long estimateCount(Object key) {
		return this.frequency.estimateCount(key.toString());
	}

	public long estimateCount(long key) {
		return this.frequency.estimateCount(key);
	}

	private void addKeyChannelState(int channel, long keyFrequencyEstimation) {
		if (keyFrequencyEstimation > channelState[channel][INDEX_KEY_HIGHEST_FREQUENCY]) {
			channelState[channel][INDEX_KEY_HIGHEST_FREQUENCY] = keyFrequencyEstimation;
		}
	}

	public long getHighestFrequency() {
		long firstHighestFrequency = 0;
		for (int i = 0; i < channelState.length; i++) {
			if (channelState[i][INDEX_KEY_HIGHEST_FREQUENCY] > firstHighestFrequency) {
				firstHighestFrequency = channelState[i][INDEX_KEY_HIGHEST_FREQUENCY];
			}
		}
		return firstHighestFrequency;
	}

	public long getFactorBetweenHighestFrequencies() {
		long firstHighestFrequency = 0;
		long secondHighestFrequency = 0;
		for (int i = 0; i < channelState.length; i++) {
			if (channelState[i][INDEX_KEY_HIGHEST_FREQUENCY] > firstHighestFrequency) {
				secondHighestFrequency = firstHighestFrequency;
				firstHighestFrequency = channelState[i][INDEX_KEY_HIGHEST_FREQUENCY];
			} else if (channelState[i][INDEX_KEY_HIGHEST_FREQUENCY] > secondHighestFrequency) {
				secondHighestFrequency = channelState[i][INDEX_KEY_HIGHEST_FREQUENCY];
			}
		}
		if (firstHighestFrequency == 0 || secondHighestFrequency == 0) {
			return 0;
		}
		return firstHighestFrequency / secondHighestFrequency;
	}

	public long getNumberOfHops() {
		return getFactorBetweenHighestFrequencies() / factor;
	}
}
