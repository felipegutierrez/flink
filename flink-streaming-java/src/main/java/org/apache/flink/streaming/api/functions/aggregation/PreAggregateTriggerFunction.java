package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import java.util.Calendar;

public class PreAggregateTriggerFunction<T> extends Thread implements PreAggregateTrigger<T> {

	private final long DEFAULT_INTERVAL_CHANGE_PRE_AGGREGATE = Time.minutes(1).toMilliseconds();
	// private final long maxCount;
	private long periodMilliseconds;
	private transient PreAggregateTriggerCallback callback;
	// private PreAggregateMqttListener preAggregateMqttListener;
	private long offsetTime;
	// private transient long count = 0;
	private boolean running = false;

	public PreAggregateTriggerFunction(long periodMilliseconds) {
		Preconditions.checkArgument(periodMilliseconds >= 0, "periodMilliseconds must be equal or greater than 0");
		// this.maxCount = -1;
		this.periodMilliseconds = periodMilliseconds;
		this.running = true;
		this.offsetTime = Calendar.getInstance().getTimeInMillis();
	}

	// public PreAggregateTriggerFunction(long periodMilliseconds, long maxToAggregate) {
	// Preconditions.checkArgument(periodMilliseconds >= 0, "periodMilliseconds must be equal or  greater than 0");
	// Preconditions.checkArgument(maxToAggregate > 0, "maxCount must be greater than 0");
	// this.maxCount = maxToAggregate;
	// this.periodMilliseconds = periodMilliseconds;
	// }

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
		// if (maxCount != -1) {
		// count++;
		// if (count >= maxCount) {
		// 	collect();
		// 	}
		// }
	}

	@Override
	public void reset() {
		// this.count = 0;
	}

	// private synchronized void collect() {
	private void collect() {
		try {
			callback.collect();
			reset();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String explain() {
		return "PreAggregateTrigger";//  + maxCount;
	}

	@Override
	public void run() {
		while (running) {
			if (this.periodMilliseconds > 0) {
				delay(this.periodMilliseconds);
				System.out.println("collect");
			}
			collect();
		}
	}

	private void delay(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public long getPeriodMilliseconds() {
		return periodMilliseconds;
	}

	public void setPeriodMilliseconds(long periodMilliseconds) {
		System.out.println("new periodMilliseconds set: " + periodMilliseconds);
		this.periodMilliseconds = periodMilliseconds;
	}
}
