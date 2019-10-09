package org.apache.flink.streaming.api.functions.combiner;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

public class CombinerTriggerFunction<T> implements CombinerTrigger<T> {
	private static final Logger logger = LoggerFactory.getLogger(CombinerTriggerFunction.class);

	private final long maxCount;
	private transient CombinerTriggerCallback callback;
	private transient long count = 0;
	private transient long startTime;
	private transient long timeout;

	public CombinerTriggerFunction(long maxToCombine) {
		this(maxToCombine, 20);
	}

	public CombinerTriggerFunction(long maxToCombine, long secondsTimeout) {
		Preconditions.checkArgument(maxToCombine > 0, "maxCount must be greater than 0");
		this.maxCount = maxToCombine;
		this.timeout = secondsTimeout;
		this.startTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public void registerCallback(CombinerTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
		this.startTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public void onElement(T element) throws Exception {
		count++;
		long beforeTime = Calendar.getInstance().getTimeInMillis() - Time.seconds(timeout).toMilliseconds();
		if (count >= maxCount || beforeTime >= startTime) {
			callback.finishMerge();
			reset();
		}
	}

	@Override
	public void reset() {
		count = 0;
	}

	@Override
	public String explain() {
		return "CountBundleTrigger with size " + maxCount;
	}
}
