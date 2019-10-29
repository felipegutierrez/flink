package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.util.Preconditions;

import java.util.TimerTask;

public class PreAggregateTriggerFunction<T> extends TimerTask implements PreAggregateTrigger<T> {

	private final long maxCount;
	private transient PreAggregateTriggerCallback callback;
	private transient long count = 0;
	private final long periodSeconds;

	public PreAggregateTriggerFunction(long periodSeconds) {
		Preconditions.checkArgument(periodSeconds > 0, "periodSeconds must be greater than 0");
		this.maxCount = -1;
		this.periodSeconds = periodSeconds;
	}

	public PreAggregateTriggerFunction(long periodSeconds, long maxToAggregate) {
		Preconditions.checkArgument(maxToAggregate > 0, "maxCount must be greater than 0");
		Preconditions.checkArgument(periodSeconds > 0, "periodSeconds must be greater than 0");
		this.maxCount = maxToAggregate;
		this.periodSeconds = periodSeconds;
	}

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
		count++;
		if (maxCount != -1 && count >= maxCount) {
			callback.collect();
			reset();
		}
	}

	@Override
	public void reset() {
		this.count = 0;
	}

	@Override
	public String explain() {
		return "PreAggregateTrigger with size " + maxCount;
	}

	@Override
	public void run() {
		try {
			callback.collect();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			reset();
		}
	}

	public long getPeriodSeconds() {
		return periodSeconds;
	}
}
