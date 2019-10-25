package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import java.util.Calendar;

public class PreAggregateTriggerFunction<T> implements PreAggregateTrigger<T> {

	// private final long maxCount;
	private transient PreAggregateTriggerCallback callback;
	// private transient long count = 0;
	private transient long offsetTime;
	private transient long timeout;

	public PreAggregateTriggerFunction(long secondsTimeout) {
		// Preconditions.checkArgument(maxToCombine > 0, "maxCount must be greater than 0");
		// this.maxCount = maxToCombine;
		this.timeout = secondsTimeout;
		this.offsetTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
		this.offsetTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public void onElement(T element) throws Exception {
		// count++;
		long elapsedTime = Calendar.getInstance().getTimeInMillis() - Time.seconds(timeout).toMilliseconds();
		// if (count >= maxCount || beforeTime >= startTime) {
		if (elapsedTime >= offsetTime) {
			callback.collect();
			reset();
		}
	}

	@Override
	public void reset() {
		// count = 0;
	}

	@Override
	public String explain() {
		return "PreAggregateTrigger with size ";
		// + maxCount;
	}
}
