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
		double outPoolUsage075 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.75);
		double outPoolUsage095 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.95);
		double outPoolUsage098 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.98);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();
		int newMinCount = currentMinCount;
		String label = "=";

		if (this.enableController) {
			int percent05 = (int) Math.ceil(currentMinCount * 0.05);
			int percent30 = (int) Math.ceil(currentMinCount * 0.3);

			// Update the parameter K of the Pre-aggregation operator
			if (outPoolUsageMean >= 27.0 && outPoolUsage099 >= 50.0) {
				// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
				if (outPoolUsageMin == 100 && outPoolUsageMax == 100) {
					if (currentMinCount < 20) {
						newMinCount = currentMinCount * 2;
						label = "+++";
					} else {
						newMinCount = currentMinCount + percent30;
						label = "++";
					}
				} else {
					newMinCount = currentMinCount + percent05;
					label = "+";
				}
				this.currentCapacity = this.numRecordsOutPerSecond;
			} else if (outPoolUsageMean < 27.0 && outPoolUsage075 < 31.0 && outPoolUsage095 < 37.0) {
				// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
				if (outPoolUsageMin == 25 && outPoolUsageMax == 25) {
					newMinCount = currentMinCount - percent30;
					label = "--";
				} else {
					// if the output throughput is greater than the capacity we don't decrease the parameter K
					if (this.currentCapacity == 0 || this.numRecordsOutPerSecond < this.currentCapacity) {
						newMinCount = currentMinCount - percent05;
						label = "-";
					}
				}
			}
		}

		String msg = "Controller[" + this.subtaskIndex + "]" +
			" OutPoolUsg min[" + outPoolUsageMin + "]max[" + outPoolUsageMax + "]mean[" + outPoolUsageMean +
			"]0.5[" + outPoolUsage05 + "]0.75[" + outPoolUsage075 + "]0.95[" + outPoolUsage095 + "]0.98[" + outPoolUsage098 +
			"]0.99[" + outPoolUsage099 + "]StdDev[" + df.format(outPoolUsageStdDev) + "]" +
			" Latency min[" + latencyMin + "]max[" + latencyMax + "]mean[" + latencyMean +
			"]0.5[" + latencyQuantile05 + "]0.99[" + latencyQuantile099 + "]StdDev[" + df.format(latencyStdDev) + "]" +
			" IN_prev[" + df.format(this.numRecordsInPerSecondPrev) + "]IN[" + df.format(this.numRecordsInPerSecond) +
			"] OUT[" + df.format(this.numRecordsOutPerSecond) + "]threshold[" + df.format(this.currentCapacity) + "]" +
			" K" + label + ": " + currentMinCount + "->" + newMinCount;
		System.out.println(msg);

		// Update the previous input throughput of the operator
		this.numRecordsInPerSecondPrev = this.numRecordsInPerSecond;

		// Update parameters to Prometheus+Grafana
		this.preAggParamGauge.updateValue(newMinCount);
		this.preAggLatencyMeanGauge.updateValue(latencyMean);

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
