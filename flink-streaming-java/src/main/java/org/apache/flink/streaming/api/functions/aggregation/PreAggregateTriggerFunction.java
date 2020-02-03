package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import java.util.Calendar;

public class PreAggregateTriggerFunction<T> extends Thread implements PreAggregateTrigger<T> {

	private final long DEFAULT_INTERVAL_CHANGE_PRE_AGGREGATE = Time.minutes(1).toMilliseconds();
	private long periodMilliseconds;
	private transient PreAggregateTriggerCallback callback;
	// private PreAggregateMqttListener preAggregateMqttListener;
	private long offsetTime;
	private boolean running = false;

	public PreAggregateTriggerFunction(long periodMilliseconds) {
		Preconditions.checkArgument(periodMilliseconds > 0, "periodMilliseconds must be greater than 0");
		this.periodMilliseconds = periodMilliseconds;
		this.running = true;
		this.offsetTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public void registerCallback(PreAggregateTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
	}

	@Override
	public void reset() {
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
				// System.out.println("collect");
				try {
					callback.collect();
					// reset();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
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
