package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import java.util.Calendar;

public class PreAggregateTriggerFunction<T> implements PreAggregateTrigger<T> {

	private final long maxCount;
	private transient PreAggregateTriggerCallback callback;
	private transient long count = 0;
	private transient long offsetTime;
	private transient long timeout;

	public PreAggregateTriggerFunction(long secondsTimeout) {
		this.maxCount = -1;
		this.timeout = secondsTimeout;
		this.offsetTime = Calendar.getInstance().getTimeInMillis();
	}

	public PreAggregateTriggerFunction(long secondsTimeout, long maxToAggregate) {
		Preconditions.checkArgument(maxToAggregate > 0, "maxCount must be greater than 0");
		this.maxCount = maxToAggregate;
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
		count++;
		long elapsedTime = Calendar.getInstance().getTimeInMillis() - Time.seconds(timeout).toMilliseconds();
		if ((maxCount != -1 && count >= maxCount) || elapsedTime >= offsetTime) {
			callback.collect();
			reset();
		}
	}

	@Override
	public void reset() {
		this.count = 0;
		this.offsetTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public String explain() {
		return "PreAggregateTrigger with size " + maxCount;
	}
}
