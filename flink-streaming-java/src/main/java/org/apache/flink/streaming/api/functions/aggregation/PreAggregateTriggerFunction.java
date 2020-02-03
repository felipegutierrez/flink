package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.util.Preconditions;

public class PreAggregateTriggerFunction<T> implements PreAggregateTrigger<T> {

	private long maxCount;
	private transient long count = 0;
	private transient PreAggregateTriggerCallback callback;

	public PreAggregateTriggerFunction(long maxCount) {
		Preconditions.checkArgument(maxCount > 0, "periodMilliseconds must be greater than 0");
		this.maxCount = maxCount;
	}

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
		count++;
		if (count >= maxCount) {
			callback.collect();
			reset();
		}
	}

	@Override
	public void reset() {
		count = 0;
	}

	@Override
	public String explain() {
		return "maxCount [" + maxCount + "]";
	}

	public long getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(long maxCount) {
		System.out.println("new maxCount set: " + maxCount);
		this.maxCount = maxCount;
	}
}
