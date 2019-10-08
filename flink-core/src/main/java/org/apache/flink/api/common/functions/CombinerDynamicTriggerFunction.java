package org.apache.flink.api.common.functions;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.IFrequency;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

public class CombinerDynamicTriggerFunction<K, T> implements CombinerDynamicTrigger<K, T> {
	private static final Logger logger = LoggerFactory.getLogger(CombinerDynamicTriggerFunction.class);

	private final long LIMIT_MIN_COUNT = 1;
	private final long INCREMENT = 10;

	private long maxCount;
	private transient long count = 0;
	private transient long timeout;
	private transient CombinerTriggerCallback callback;
	private transient IFrequency frequency;
	private transient long maxFrequencyCMS = 0;
	private transient long startTime;

	public CombinerDynamicTriggerFunction() {
		this(20);
	}

	public CombinerDynamicTriggerFunction(long secondsTimeout) {
		initFrequencySketch();
		this.maxCount = LIMIT_MIN_COUNT;
		this.startTime = Calendar.getInstance().getTimeInMillis();
		this.timeout = secondsTimeout;
		Preconditions.checkArgument(this.maxCount > 0, "maxCount must be greater than 0");
	}

	@Override
	public void registerCallback(CombinerTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
		this.startTime = Calendar.getInstance().getTimeInMillis();
		initFrequencySketch();
	}

	/**
	 * The Combiner is triggered when the count reaches the maxCount or by a timeout
	 */
	@Override
	public void onElement(K key, T element) throws Exception {
		// add key element on the HyperLogLog to infer the data-stream cardinality
		this.frequency.add(key.toString(), 1);
		long itemCMS = this.frequency.estimateCount(key.toString());
		if (itemCMS > this.maxFrequencyCMS) {
			this.maxFrequencyCMS = itemCMS;
		}
		count++;
		long beforeTime = Calendar.getInstance().getTimeInMillis() - Time.seconds(timeout).toMilliseconds();
		if (count >= maxCount || beforeTime >= startTime) {
			callback.finishMerge();
		}
	}

	@Override
	public void reset() throws Exception {
		if (count != 0) {
			String msg = "Thread[" + Thread.currentThread().getId() + "] frequencyCMS[" + maxFrequencyCMS + "] maxCount[" + maxCount + "]";
			if (maxFrequencyCMS > maxCount + INCREMENT) {
				// It is necessary to increase the combiner
				long diff = maxFrequencyCMS - maxCount;
				maxCount = maxCount + diff;
				msg = msg + " - INCREASING >>>";
				resetFrequencySketch();
			} else if (maxFrequencyCMS < maxCount - INCREMENT) {
				// It is necessary to reduce the combiner
				maxCount = maxFrequencyCMS + INCREMENT;
				msg = msg + " - DECREASING <<<";
				resetFrequencySketch();
			} else {
				msg = msg + " - HOLDING";
				this.startTime = Calendar.getInstance().getTimeInMillis();
			}
			System.out.println(msg);
			// logger.info(msg);
		}
		count = 0;
	}

	private void initFrequencySketch() {
		if (this.frequency == null) {
			this.frequency = new CountMinSketch(10, 5, 0);
		}
		this.maxFrequencyCMS = 0;
		this.startTime = Calendar.getInstance().getTimeInMillis();
	}

	private void resetFrequencySketch() {
		this.frequency = new CountMinSketch(10, 5, 0);
		this.maxFrequencyCMS = 0;
		this.startTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public String explain() {
		return "CountCombinerTriggerDynamic with size " + maxCount;
	}
}
