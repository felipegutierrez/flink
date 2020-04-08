package org.apache.flink.streaming.util.functions;

import org.apache.flink.metrics.Gauge;

public class PreAggLatencyMeanGauge implements Gauge<Double> {

	private double preAggLatencyMean;

	@Override
	public Double getValue() {
		return preAggLatencyMean;
	}

	public void updateValue(double preAggLatencyMean) {
		this.preAggLatencyMean = preAggLatencyMean;
	}
}
