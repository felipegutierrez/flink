package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.util.Preconditions;

public class PreAggregateTriggerFunction<T> implements PreAggregateTrigger<T> {

	private final PreAggregateStrategy preAggregateStrategy;
	private final long previousTime;
	private int maxCount;
	private transient int count = 0;
	private transient PreAggregateTriggerCallback callback;

	public PreAggregateTriggerFunction(int maxCount) {
		Preconditions.checkArgument(maxCount > 0, "pre-aggregation count must be greater than 0");
		this.maxCount = maxCount;
		this.preAggregateStrategy = PreAggregateStrategy.GLOBAL;
		this.previousTime = System.currentTimeMillis();
	}

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
		this.count++;
		if (this.count >= this.maxCount) {
			this.callback.collect();
			reset();
		}
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
