package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PreAggregateTimer extends Thread implements Serializable {

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private final PreAggregateTriggerFunction preAggregateTriggerFunction;
	private final int subtaskIndex;
	private final int windowTimeMilliseconds;
	private long elapsedTime;
	private boolean running = false;

	public PreAggregateTimer(PreAggregateTriggerFunction preAggregateTriggerFunction, int subtaskIndex) {
		Preconditions.checkArgument(preAggregateTriggerFunction.getMaxTime() > 0, "pre-aggregation timeout must be greater than 1 second.");
		this.preAggregateTriggerFunction = preAggregateTriggerFunction;
		this.windowTimeMilliseconds = (int) (this.preAggregateTriggerFunction.getMaxTime() * 1000);
		this.subtaskIndex = subtaskIndex;
		this.running = true;
	}

	public void run() {
		try {
			while (running) {
				Thread.sleep(200);
				long currentTimeMillis = System.currentTimeMillis();
				if (currentTimeMillis > (this.elapsedTime + windowTimeMilliseconds)) {
					// System.out.println("subtaskIndex[" + subtaskIndex + "] elapsed time: " + sdf.format(new Date(currentTimeMillis)));
					// trigger the combiner based on the timeout window
					this.preAggregateTriggerFunction.timeTrigger();
					this.elapsedTime = System.currentTimeMillis();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cancel() {
		this.running = false;
	}

	private void disclaimer() {
		System.out.println();
		System.out.println();
	}
}
