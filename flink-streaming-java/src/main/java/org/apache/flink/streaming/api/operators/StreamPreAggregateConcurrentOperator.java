package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateConcurrentFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateTriggerFunction;

public class StreamPreAggregateConcurrentOperator<K, V, IN, OUT> extends AbstractUdfStreamPreAggregateConcurrentOperator<K, V, IN, OUT> {
	private static final long serialVersionUID = 1L;

	/**
	 * KeySelector is used to extract key for bundle map.
	 */
	private final KeySelector<IN, K> keySelector;

	public StreamPreAggregateConcurrentOperator(PreAggregateConcurrentFunction<K, V, IN, OUT> function,
												PreAggregateTriggerFunction<IN> preAggregateTriggerFunction,
												KeySelector<IN, K> keySelector,
												boolean enableController) {
		super(function, preAggregateTriggerFunction, enableController);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}
