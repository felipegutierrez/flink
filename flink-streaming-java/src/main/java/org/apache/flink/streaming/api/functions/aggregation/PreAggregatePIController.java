package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.metrics.Histogram;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PreAggregatePIController extends Thread implements Serializable {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private PreAggregateTriggerFunction preAggregateTriggerFunction;
	private long elapsedTime;
	private long seconds;
	private int subtaskIndex;
	private long latencyMilliseconds;
	private float outPoolUsage;
	private boolean running = false;

	/**
	 * Histogram metrics to monitor latency and network buffer
	 */
	private Histogram latencyHistogram;
	private Histogram outPoolUsageHistogram;

	public PreAggregatePIController(PreAggregateTriggerFunction preAggregateTriggerFunction,
									Histogram latencyHistogram, Histogram outPoolUsageHistogram,
									int subtaskIndex) {
		this(preAggregateTriggerFunction, latencyHistogram, outPoolUsageHistogram, subtaskIndex, 20);
	}

	public PreAggregatePIController(PreAggregateTriggerFunction preAggregateTriggerFunction,
									Histogram latencyHistogram, Histogram outPoolUsageHistogram,
									int subtaskIndex, long seconds) {
		this.preAggregateTriggerFunction = preAggregateTriggerFunction;
		this.latencyHistogram = latencyHistogram;
		this.outPoolUsageHistogram = outPoolUsageHistogram;
		this.subtaskIndex = subtaskIndex;
		this.seconds = seconds;
		this.running = true;
		this.disclaimer();
	}

	private void disclaimer() {
		System.out.println("Initialized pre-aggregate PI Controller for subtask[" + this.subtaskIndex + "] scheduled to every " + this.seconds + " seconds.");
	}

	public void run() {
		try {
			while (running) {
				Thread.sleep(seconds * 1000);

				long newMaxCountPreAggregate = this.computePreAggregateParameter();
				this.preAggregateTriggerFunction.setMaxCount(newMaxCountPreAggregate, this.subtaskIndex);

				System.out.println("PI Controller for subtask[" + this.subtaskIndex + "] at " + sdf.format(new Date(System.currentTimeMillis())));
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
	private long computePreAggregateParameter() {
		double latencyQuantile05 = this.latencyHistogram.getStatistics().getQuantile(0.5);
		double latencyQuantile099 = this.latencyHistogram.getStatistics().getQuantile(0.99);
		double latencyStdDev = this.latencyHistogram.getStatistics().getStdDev();
		System.out.println("Latency metrics: quantiles 0.5[" + latencyQuantile05 + "] 0.99[" + latencyQuantile099 + "] stdDev[" + latencyStdDev + "]");

		double outPoolUsage05 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.5);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();
		System.out.println("outPoolUsage metrics: quantiles 0.5[" + outPoolUsage05 + "] 0.99[" + outPoolUsage099 + "] stdDev[" + outPoolUsageStdDev + "]");

		return -1;
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
