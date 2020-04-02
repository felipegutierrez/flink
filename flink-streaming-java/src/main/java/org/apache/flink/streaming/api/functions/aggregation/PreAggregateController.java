package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.util.functions.PreAggParamGauge;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.text.SimpleDateFormat;

public class PreAggregateController extends Thread implements Serializable {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private PreAggregateTriggerFunction preAggregateTriggerFunction;
	private long elapsedTime;
	private int controllerFrequencySec;
	private int subtaskIndex;
	private long latencyMilliseconds;
	private float outPoolUsage;
	private boolean running = false;

	/**
	 * Histogram and Gauge metrics to monitor latency and network buffer
	 */
	private Histogram latencyHistogram;
	private Histogram outPoolUsageHistogram;
	private PreAggParamGauge preAggParamGauge;

	public PreAggregateController(PreAggregateTriggerFunction preAggregateTriggerFunction,
								  Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  PreAggParamGauge preAggParamGauge, int subtaskIndex) {
		this(preAggregateTriggerFunction, latencyHistogram, outPoolUsageHistogram, preAggParamGauge, subtaskIndex, 60);
	}

	public PreAggregateController(PreAggregateTriggerFunction preAggregateTriggerFunction,
								  Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  PreAggParamGauge preAggParamGauge, int subtaskIndex, int controllerFrequencySec) {
		Preconditions.checkArgument(controllerFrequencySec >= 60, "The controller frequency must be greater than 60 seconds");
		Preconditions.checkArgument(controllerFrequencySec <= (60 * 10), "The controller frequency  must be less than 600 seconds");
		this.preAggregateTriggerFunction = preAggregateTriggerFunction;
		this.latencyHistogram = latencyHistogram;
		this.outPoolUsageHistogram = outPoolUsageHistogram;
		this.preAggParamGauge = preAggParamGauge;
		this.subtaskIndex = subtaskIndex;
		this.controllerFrequencySec = controllerFrequencySec;
		this.running = true;
		this.disclaimer();
	}

	private void disclaimer() {
		System.out.println("Initialized pre-aggregate Controller for subtask[" + this.subtaskIndex
			+ "] scheduled to every " + this.controllerFrequencySec + " seconds.");
	}

	public void run() {
		try {
			while (running) {
				Thread.sleep(controllerFrequencySec * 1000);

				int newMaxCountPreAggregate = this.computePreAggregateParameter(this.preAggregateTriggerFunction.getMaxCount());
				this.preAggregateTriggerFunction.setMaxCount(newMaxCountPreAggregate, this.subtaskIndex);
				// System.out.println("Controller for subtask[" + this.subtaskIndex + "] at " + sdf.format(new Date(System.currentTimeMillis())));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Computes the new pre-aggregate parameter to achieve minimum emission latency to the pre-aggregate operator.
	 *
	 * @return
	 */
	private int computePreAggregateParameter(int currentMaxCount) {
		double latencyQuantile05 = this.latencyHistogram.getStatistics().getQuantile(0.5);
		double latencyQuantile099 = this.latencyHistogram.getStatistics().getQuantile(0.99);
		double latencyStdDev = this.latencyHistogram.getStatistics().getStdDev();

		double outPoolUsage05 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.5);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();

		int percent05 = (int) Math.ceil(currentMaxCount * 0.05);
		int newMaxCount = currentMaxCount;
		if (outPoolUsage099 >= 50.0 || outPoolUsage05 >= 35.0) {
			// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
			newMaxCount = currentMaxCount + percent05;
		} else if (outPoolUsage099 <= 25.0 && outPoolUsage05 <= 25.0) {
			// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
			newMaxCount = currentMaxCount - percent05;
		}

		String msg = "Controller[" + this.subtaskIndex + "] " +
			" - Latency: quantile 0.5[" + latencyQuantile05 + "] 0.99[" + latencyQuantile099 + "] stdDev[" + latencyStdDev + "]" +
			" - OutPoolUsage: quantile 0.5[" + outPoolUsage05 + "] 0.99[" + outPoolUsage099 + "] stdDev[" + outPoolUsageStdDev + "]" +
			" - currentMaxCount: " + currentMaxCount + " - newMaxCount: " + newMaxCount;
		System.out.println(msg);

		if (currentMaxCount != newMaxCount) {
			this.preAggParamGauge.updateValue(newMaxCount);
			return newMaxCount;
		} else {
			return -1;
		}
	}

	/**
	 * Update the latency (time to aggregate and emit items) on the pre-aggregate operator and
	 * the output network buffer (outPoolUsage) for each output channel.
	 *
	 * @param latencyMilliseconds
	 * @param outPoolUsage
	 */
	public void updateMonitoredValues(long latencyMilliseconds, float outPoolUsage) {
		this.latencyMilliseconds = latencyMilliseconds;
		this.outPoolUsage = outPoolUsage;
	}

	public void cancel() {
		this.running = false;
	}

	public Histogram getLatencyHistogram() {
		return latencyHistogram;
	}

	public Histogram getOutPoolUsageHistogram() {
		return outPoolUsageHistogram;
	}
}
