package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.util.Preconditions;

public class PreAggregateTriggerFunction<T> implements PreAggregateTrigger<T> {

	private PreAggregateStrategy preAggregateStrategy;
	private int minCount;
	private transient int count = 0;
	private transient PreAggregateTriggerCallback callback;

	public PreAggregateTriggerFunction(int minCount) {
		Preconditions.checkArgument(minCount > 0, "periodMilliseconds must be greater than 0");
		this.minCount = minCount;
		this.preAggregateStrategy = PreAggregateStrategy.GLOBAL;
	}

	public PreAggregateTriggerFunction(int minCount, PreAggregateStrategy preAggregateStrategy) {
		Preconditions.checkArgument(minCount > 0, "periodMilliseconds must be greater than 0");
		this.minCount = minCount;
		this.preAggregateStrategy = preAggregateStrategy;
	}

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
		this.count++;
		if (this.count >= this.minCount) {
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
		return "minCount [" + this.minCount + "]";
	}

	@Override
	public int getMaxCount() {
		return this.minCount;
	}

	@Override
	public void setMaxCount(int minCount, int subtaskIndex) {
		if (minCount > -1) {
			if (subtaskIndex == -1) {
				System.out.println("Subtask[all] - new maxCount set: " + minCount);
			} else {
				System.out.println("Subtask[" + subtaskIndex + "] - new maxCount set: " + minCount);
			}
			this.minCount = minCount;
		} else {
			if (subtaskIndex == -1) {
				System.out.println("Warning: attempt to set maxCount failed[" + minCount + "] for Subtask[all]");
			} else {
				System.out.println("Warning: attempt to set maxCount failed[" + minCount + "] for Subtask[" + subtaskIndex + "]");
			}
		}
	}

	@Override
	public PreAggregateStrategy getPreAggregateStrategy() {
		return this.preAggregateStrategy;
	}
}
