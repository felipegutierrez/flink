package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.util.functions.PreAggParamGauge;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class PreAggregateController extends Thread implements Serializable {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private DecimalFormat df = new DecimalFormat("#.###");

	private PreAggregateTriggerFunction preAggregateTriggerFunction;
	private int controllerFrequencySec;
	private int subtaskIndex;
	private boolean running = false;

	/**
	 * Histogram and Gauge metrics to monitor latency and network buffer
	 */
	private Histogram latencyHistogram;
	private Histogram outPoolUsageHistogram;
	private PreAggParamGauge preAggParamGauge;
	private double numRecordsOutPerSecond;

	public PreAggregateController(PreAggregateTriggerFunction preAggregateTriggerFunction,
								  Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  PreAggParamGauge preAggParamGauge, int subtaskIndex) {
		this(preAggregateTriggerFunction, latencyHistogram, outPoolUsageHistogram, preAggParamGauge, subtaskIndex, 180);
	}

	public PreAggregateController(PreAggregateTriggerFunction preAggregateTriggerFunction,
								  Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  PreAggParamGauge preAggParamGauge, int subtaskIndex, int controllerFrequencySec) {
		Preconditions.checkArgument(controllerFrequencySec >= 180, "The controller frequency must be greater than 180 seconds");
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
		long latencyQuantileMin = this.latencyHistogram.getStatistics().getMin();
		long latencyQuantileMax = this.latencyHistogram.getStatistics().getMax();
		double latencyQuantileMean = this.latencyHistogram.getStatistics().getMean();
		double latencyQuantile05 = this.latencyHistogram.getStatistics().getQuantile(0.5);
		double latencyQuantile099 = this.latencyHistogram.getStatistics().getQuantile(0.99);
		double latencyStdDev = this.latencyHistogram.getStatistics().getStdDev();

		long outPoolUsageMin = this.outPoolUsageHistogram.getStatistics().getMin();
		long outPoolUsageMax = this.outPoolUsageHistogram.getStatistics().getMax();
		double outPoolUsageMean = this.outPoolUsageHistogram.getStatistics().getMean();
		double outPoolUsage05 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.5);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();

		int percent05 = (int) Math.ceil(currentMaxCount * 0.05);
		int newMaxCount = currentMaxCount;
		String label = "=";
		if (outPoolUsage099 >= 50.0 || outPoolUsageMean >= 30.0) {
			// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
			newMaxCount = currentMaxCount + percent05;
			label = "++";
		} else if (outPoolUsage099 <= 25.0 && outPoolUsageMean <= 25.0) {
			// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
			newMaxCount = currentMaxCount - percent05;
			label = "--";
		}

		String msg = "Controller[" + this.subtaskIndex + "]" +
			" OutPoolUsg min[" + outPoolUsageMin + "]max[" + outPoolUsageMax + "]mean[" + outPoolUsageMean +
			"]0.5[" + outPoolUsage05 + "]0.99[" + outPoolUsage099 + "]StdDev[" + df.format(outPoolUsageStdDev) + "]" +
			" Latency min[" + latencyQuantileMin + "]max[" + latencyQuantileMax + "]mean[" + latencyQuantileMean +
			"]0.5[" + latencyQuantile05 + "]0.99[" + latencyQuantile099 + "] StdDev[" + df.format(latencyStdDev) + "]" +
			"numRecordsOutPerSecond[" + this.numRecordsOutPerSecond + "]" + " K" + label + "" + currentMaxCount + "->" + newMaxCount;
		System.out.println(msg);

		if (currentMaxCount != newMaxCount) {
			this.preAggParamGauge.updateValue(newMaxCount);
			return newMaxCount;
		} else {
			return -1;
		}
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

	public void setNumRecordsOutPerSecond(double numRecordsOutPerSecond) {
		this.numRecordsOutPerSecond = numRecordsOutPerSecond;
	}
}
