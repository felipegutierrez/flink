package org.apache.flink.streaming.util.functions;

import org.apache.flink.metrics.Gauge;

public class PreAggParamGauge implements Gauge<Integer> {

	private int preAggParam;

	@Override
	public Integer getValue() {
		return preAggParam;
	}

	public void updateValue(int preAggParam) {
		this.preAggParam = preAggParam;
	}
}
