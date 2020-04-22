package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.util.Preconditions;

public class PreAggregateTriggerFunction<T> implements PreAggregateTrigger<T> {

	public static final String TOPIC_PRE_AGGREGATE_PARAMETER = "topic-pre-aggregate-parameter";
	private final PreAggregateStrategy preAggregateStrategy;
	private long previousTime;
	private int maxCount;
	private transient int count = 0;
	private transient PreAggregateTriggerCallback callback;

	public PreAggregateTriggerFunction(int maxCount) {
		this(maxCount, PreAggregateStrategy.GLOBAL);
	}

	public PreAggregateTriggerFunction(int maxCount, PreAggregateStrategy preAggregateStrategy) {
		Preconditions.checkArgument(maxCount > 0, "pre-aggregation count must be greater than 0");
		this.maxCount = maxCount;
		this.preAggregateStrategy = preAggregateStrategy;
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
	public void setMaxCount(int newMaxCount, int subtaskIndex, PreAggregateStrategy strategy) {
		if (newMaxCount > -1) {
			if (PreAggregateStrategy.LOCAL.equals(strategy)) {
				if (this.maxCount != newMaxCount) {
					System.out.println("Subtask[" + subtaskIndex + "] - new maxCount set[" + strategy.getValue() + "]: " + newMaxCount);
					this.maxCount = newMaxCount;
				} else {
					System.out.println("Subtask[" + subtaskIndex + "] - maxCount not changed[" + strategy.getValue() + "]: " + newMaxCount);
				}
			} else if (PreAggregateStrategy.GLOBAL.equals(strategy)) {
				if (System.currentTimeMillis() - this.previousTime > 10000) {
					// It has been more than 10 seconds that this method was called so we update the this.maxCount anyway
					this.previousTime = System.currentTimeMillis();
					if (this.maxCount != newMaxCount) {
						System.out.println("New schedule - Subtask[" + subtaskIndex + "] - new maxCount set[" + strategy.getValue() + "]: " + newMaxCount);
						this.maxCount = newMaxCount;
					} else {
						System.out.println("New schedule - Subtask[" + subtaskIndex + "] - maxCount not changed[" + strategy.getValue() + "]: " + newMaxCount);
					}
				} else {
					// It has been less than 10 seconds that this method was called so we decide to update this.maCount only if the value is greater than the previous value
					if (newMaxCount > this.maxCount) {
						System.out.println("Subtask[" + subtaskIndex + "] - new maxCount set[" + strategy.getValue() + "]: " + newMaxCount);
						this.maxCount = newMaxCount;
					} else {
						System.out.println("Subtask[" + subtaskIndex + "] - maxCount not changed[" + strategy.getValue() + "]: " + newMaxCount);
					}
				}
			} else {
				System.out.println("ERROR: PreAggregateStrategy [" + strategy.getValue() + "] not implemented.");
			}
		} else {
			System.out.println("Warning: attempt to set maxCount failed[" + newMaxCount + "] for Subtask[all]");
		}
	}

	@Override
	public PreAggregateStrategy getPreAggregateStrategy() {
		return this.preAggregateStrategy;
	}
}
