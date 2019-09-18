package org.apache.flink.streaming.runtime.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Partitioner that distributes the data according to the Power of both choices first implemented in Apache Storm.
 *
 * @param <T> Type of the elements in the Stream being rebalanced
 */
@Internal
public class PartialPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;
	private KeySelector<T, ?> keySelector;
	private long[] targetChannelStats;
	private int[] returnArray = new int[1];
	private boolean initializedStats;
	private HashFunction[] h;
	private int workersPerKey = 2;
	private int currentPrime = 2;

	public PartialPartitioner(KeySelector<T, ?> keySelector) {
		this.initializedStats = false;
		this.keySelector = keySelector;
	}

	public PartialPartitioner(KeySelector<T, ?> keySelector, int numWorkersPerKey) {
		this.initializedStats = false;
		this.keySelector = keySelector;
		this.workersPerKey = numWorkersPerKey;
	}

	@Override
	public void setup(int numberOfChannels) {
		super.setup(numberOfChannels);
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		// public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
		// int numChannels) {
		if (!initializedStats) {
			this.targetChannelStats = new long[numberOfChannels];
			this.initializedStats = true;
			h = new HashFunction[this.workersPerKey];
			for (int i = 0; i < this.workersPerKey; i++) {
				currentPrime = getNextPrime(currentPrime);
				h[i] = Hashing.murmur3_128(currentPrime);
			}
		}
		int[] choices;
		Object key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
			int counter = 0;
			choices = new int[this.workersPerKey];
			if (this.workersPerKey == numberOfChannels) {
				while (counter < this.workersPerKey) {
					choices[counter] = counter;
					counter++;
				}
			} else {
				while (counter < this.workersPerKey) {
					choices[counter] = (int) (Math.abs(h[counter].hashBytes(serialize(key)).asLong())
						% numberOfChannels);
					counter++;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		int selected = selectMinWorker(targetChannelStats, choices);
		targetChannelStats[selected]++;

		returnArray[0] = selected;

		System.out.println("All partitions:");
		for (int i = 0; i < returnArray.length; i++) {
			System.out.println(returnArray[i]);
		}
		System.out.println("Selected partition: " + selected);
		return selected;
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
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "POWER_OF_BOTH_CHOICES";
	}
}
