package org.apache.flink.streaming.runtime.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

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

	private long[] targetChannelStats;
	// private int[] returnArray = new int[1];
	private boolean initializedStats;
	private HashFunction[] hashFunction;
	private int workersPerKey = 2;
	private int currentPrime = 2;

	public KeyGroupPartialStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.initializedStats = false;
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
	}

	public KeyGroupPartialStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism, int numWorkersPerKey) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.initializedStats = false;
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
		this.workersPerKey = numWorkersPerKey;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		K key;
		int selected = 0;
		int[] choices;

		// Initialize statistics of the operator
		if (!initializedStats) {
			// The array targetChannelStats is the size of the number of the channels
			this.targetChannelStats = new long[numberOfChannels];
			this.initializedStats = true;
			// Initialize the hashFunction with 2 worker per key by default
			hashFunction = new HashFunction[this.workersPerKey];
			for (int i = 0; i < this.workersPerKey; i++) {
				currentPrime = getNextPrime(currentPrime);
				hashFunction[i] = Hashing.murmur3_128(currentPrime);
			}
		}

		try {
			key = keySelector.getKey(record.getInstance().getValue());
			int counter = 0;
			choices = new int[this.workersPerKey];

			// logic to populate the @choices array
			if (this.workersPerKey == numberOfChannels) {
				while (counter < this.workersPerKey) {
					choices[counter] = counter;
					counter++;
				}
			} else {
				while (counter < this.workersPerKey) {
					choices[counter] = (int) (Math.abs(hashFunction[counter].hashBytes(serialize(key)).asLong()) % numberOfChannels);
					counter++;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}

		// Default option
		selected = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);
		//selected = selectMinWorker(targetChannelStats, choices);
		// update the channel statistic array
		targetChannelStats[selected]++;

		// on the original version of Flink 2015 He used to return an array with all choices
		// returnArray[0] = selected;
		// return returnArray;

		return selected;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	private int getNextPrime(int x) {
		int num = x + 1;
		while (!isPrime(num)) {
			num++;
		}
		return num;
	}

	private boolean isPrime(int num) {
		for (int i = 2; i < num; i++) {
			if (num % i == 0) {
				return false;
			}
		}
		return true;
	}

	private int selectMinWorker(long[] loadVector, int[] choice) {
		int index = choice[0];
		for (int i = 0; i < choice.length; i++) {
			if (loadVector[choice[i]] < loadVector[index]) {
				index = choice[i];
			}
		}
		return index;
	}

	private byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
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
