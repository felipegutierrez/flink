package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.util.functions.PreAggLatencyMeanGauge;
import org.apache.flink.streaming.util.functions.PreAggParamGauge;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class PreAggregateController extends Thread implements Serializable {

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private final DecimalFormat df = new DecimalFormat("#.###");

	private final PreAggregateTriggerFunction preAggregateTriggerFunction;
	private final int subtaskIndex;
	private final int controllerFrequencySec;
	private final boolean enableController;
	/**
	 * Histogram and Gauge metrics to monitor latency and network buffer
	 */
	private final Histogram latencyHistogram;
	private final Histogram outPoolUsageHistogram;
	private final PreAggParamGauge preAggParamGauge;
	private final PreAggLatencyMeanGauge preAggLatencyMeanGauge;
	private boolean running = false;
	private double numRecordsOutPerSecond;
	private double numRecordsInPerSecond;
	private double numRecordsInPerSecondPrev;
	private double currentCapacity;

	public PreAggregateController(PreAggregateTriggerFunction preAggregateTriggerFunction,
								  Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  PreAggParamGauge preAggParamGauge, PreAggLatencyMeanGauge preAggLatencyMeanGauge,
								  int subtaskIndex) {
		this(preAggregateTriggerFunction, latencyHistogram, outPoolUsageHistogram, preAggParamGauge, preAggLatencyMeanGauge,
			subtaskIndex, 60);
	}

	public PreAggregateController(PreAggregateTriggerFunction preAggregateTriggerFunction,
								  Histogram latencyHistogram, Histogram outPoolUsageHistogram,
								  PreAggParamGauge preAggParamGauge, PreAggLatencyMeanGauge preAggLatencyMeanGauge,
								  int subtaskIndex, int controllerFrequencySec) {
		if (controllerFrequencySec != -1) {
			Preconditions.checkArgument(controllerFrequencySec >= 60, "The controller frequency must be greater than 60 seconds");
			Preconditions.checkArgument(controllerFrequencySec <= (60 * 10), "The controller frequency  must be less than 600 seconds");
			this.controllerFrequencySec = controllerFrequencySec;
			this.enableController = true;
		} else {
			this.controllerFrequencySec = 60;
			this.enableController = false;
			System.out.println("WARNING: The autonomous controller is not enabled.");
		}
		this.preAggregateTriggerFunction = preAggregateTriggerFunction;
		this.latencyHistogram = latencyHistogram;
		this.outPoolUsageHistogram = outPoolUsageHistogram;
		this.preAggParamGauge = preAggParamGauge;
		this.preAggLatencyMeanGauge = preAggLatencyMeanGauge;
		this.subtaskIndex = subtaskIndex;
		this.running = true;
		this.currentCapacity = 0.0;
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
				if (this.enableController) {
					this.preAggregateTriggerFunction.setMaxCount(newMaxCountPreAggregate, this.subtaskIndex);
				}
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
	private int computePreAggregateParameter(int currentMinCount) {
		long latencyMin = this.latencyHistogram.getStatistics().getMin();
		long latencyMax = this.latencyHistogram.getStatistics().getMax();
		double latencyMean = this.latencyHistogram.getStatistics().getMean();
		double latencyQuantile05 = this.latencyHistogram.getStatistics().getQuantile(0.5);
		double latencyQuantile099 = this.latencyHistogram.getStatistics().getQuantile(0.99);
		double latencyStdDev = this.latencyHistogram.getStatistics().getStdDev();
		long outPoolUsageMin = this.outPoolUsageHistogram.getStatistics().getMin();
		long outPoolUsageMax = this.outPoolUsageHistogram.getStatistics().getMax();
		double outPoolUsageMean = this.outPoolUsageHistogram.getStatistics().getMean();
		double outPoolUsage05 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.5);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();
		int newMinCount = currentMinCount;
		String label = "=";

		if (this.enableController) {
			int percent05 = (int) Math.ceil(currentMinCount * 0.05);
			if (outPoolUsage099 >= 50.0 || outPoolUsageMean >= 30.0) {
				// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
				if (currentMinCount <= 50) {
					newMinCount = currentMinCount * 3;
				} else {
					newMinCount = currentMinCount + percent05;
				}
				label = "++";
			} else if (outPoolUsage099 <= 25.0 && outPoolUsageMean <= 25.0) {
				// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
				if (this.numRecordsOutPerSecond > this.currentCapacity) {
					// The current throughput did not reach the full capacity of the channel yet
					this.currentCapacity = this.numRecordsOutPerSecond;
					newMinCount = currentMinCount - percent05;
					label = "--";
				} else if (this.numRecordsInPerSecond < this.numRecordsInPerSecondPrev) {
					// The current output throughput is updated in order to keep decreasing the parameter K w.r.t. the output throughput
					this.currentCapacity = this.numRecordsOutPerSecond;
					newMinCount = currentMinCount - percent05;
					label = "---";
				} else {
					label = "!-";
				}
			}
		}

		String msg = "Controller[" + this.subtaskIndex + "]" +
			" OutPoolUsg min[" + outPoolUsageMin + "]max[" + outPoolUsageMax + "]mean[" + outPoolUsageMean +
			"]0.5[" + outPoolUsage05 + "]0.99[" + outPoolUsage099 + "]StdDev[" + df.format(outPoolUsageStdDev) + "]" +
			" Latency min[" + latencyMin + "]max[" + latencyMax + "]mean[" + latencyMean +
			"]0.5[" + latencyQuantile05 + "]0.99[" + latencyQuantile099 + "] StdDev[" + df.format(latencyStdDev) + "]" +
			"IN_prev[" + this.numRecordsInPerSecondPrev + "] IN[" + this.numRecordsInPerSecond +
			"] OUT[" + this.numRecordsOutPerSecond + "] threshold[" + this.currentCapacity + "]" +
			" K" + label + ": " + currentMinCount + "->" + newMinCount;
		System.out.println(msg);

		this.numRecordsInPerSecondPrev = this.numRecordsInPerSecond;

		this.preAggParamGauge.updateValue(newMinCount);
		this.preAggLatencyMeanGauge.updateValue(latencyMean);
		// if (!this.enableController || currentMinCount != newMinCount) {
		return newMinCount;
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

	public void setNumRecordsInPerSecond(double numRecordsInPerSecond) {
		this.numRecordsInPerSecond = numRecordsInPerSecond;
	}
}
