package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/**
 * Partitioner that distributes the data according to the Power of both choices first implemented in Apache Storm.
 *
 * @param <T> Type of the elements in the Stream being rebalanced
 */
@Internal
public class KeyGroupPartialStreamPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	private static final long serialVersionUID = 1L;
	private KeySelector<T, K> keySelector;
	private int maxParallelism;

	public KeyGroupPartialStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		K key;
		int hops = 0;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
			frequency.add(key.toString(), 1L);
			if (frequency.estimateCount(key.toString()) >= 5) {
				hops = 1;
			}
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels, hops);
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "PARTIAL";
	}

	@Override
	public void configure(int maxParallelism) {
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
	}
}
