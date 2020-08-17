package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.util.Preconditions;

public class PreAggregateTriggerFunction<T> implements PreAggregateTrigger<T> {

	private final PreAggregateStrategy preAggregateStrategy;
	private final long maxTime;
	private int maxCount;
	private transient int count = 0;
	private transient PreAggregateTriggerCallback callback;

	public PreAggregateTriggerFunction() {
		this(1);
	}

	public PreAggregateTriggerFunction(int maxCount) {
		Preconditions.checkArgument(maxCount > 0, "when pre-aggregation time is not considered the pre-aggregation count must be greater than 0");
		this.maxCount = maxCount;
		this.maxTime = -1L;
		this.preAggregateStrategy = PreAggregateStrategy.GLOBAL;
	}

	public PreAggregateTriggerFunction(long maxTime) {
		Preconditions.checkArgument(maxTime > 0, "when pre-aggregation is static maxTime must be greater than 0.");
		this.maxCount = -1;
		this.maxTime = maxTime;
		this.preAggregateStrategy = PreAggregateStrategy.GLOBAL;
	}

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
		if (this.maxCount > 0) {
			this.count++;
			if (this.count >= this.maxCount) {
				this.callback.collect();
				reset();
			}
		}
	}

	public void timeTrigger() throws Exception {
		this.callback.collect();
	}

	@Override
	public void reset() {
		this.count = 0;
	}

	@Override
	public String explain() {
		return "maxCount [" + this.maxCount + "]";
	}

	@Override
	public int getMaxCount() {
		return this.maxCount;
	}

	@Override
	public long getMaxTime() {
		return this.maxTime;
	}

	@Override
	public void setMaxCount(int newMaxCount, int subtaskIndex) {
		if (newMaxCount > 0) {
			if (this.maxCount != newMaxCount) {
				System.out.println("Subtask[" + subtaskIndex + "] - new maxCount: " + newMaxCount);
				this.maxCount = newMaxCount;
			} else {
				System.out.println("Subtask[" + subtaskIndex + "] - maxCount not changed]: " + newMaxCount);
			}
		} else {
			System.out.println("Warning: attempt to set maxCount failed for Subtask[" + subtaskIndex + "]: " + newMaxCount);
		}
	}

	@Override
	public PreAggregateStrategy getPreAggregateStrategy() {
		return this.preAggregateStrategy;
	}
}
