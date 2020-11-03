package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.util.functions.PreAggParamGauge;

import java.io.Serializable;

public class PreAggregateMonitor extends Thread implements Serializable {

	/** Histogram metrics to monitor network buffer */
	private final Histogram outPoolUsageHistogram;
	/** Gauge metrics to monitor latency parameter */
	private final PreAggParamGauge preAggParamGauge;
	private final int controllerFrequencySec;
	private final int subtaskId;
	private final boolean enableController;
	private boolean running = false;

	public PreAggregateMonitor(
		Histogram outPoolUsageHistogram,
		PreAggParamGauge preAggParamGauge,
		String jobManagerAddress,
		int subtaskId,
		boolean enableController) {

		this.outPoolUsageHistogram = outPoolUsageHistogram;
		this.preAggParamGauge = preAggParamGauge;
		this.running = true;
		this.controllerFrequencySec = 60;
		this.subtaskId = subtaskId;
		this.enableController = enableController;
	}

	@Override
	public void run() {
		try {
			while (running) {
				Thread.sleep(controllerFrequencySec * 1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
