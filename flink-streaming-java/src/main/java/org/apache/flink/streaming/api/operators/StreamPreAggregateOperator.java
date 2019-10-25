package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateTrigger;

public class StreamPreAggregateOperator<K, V, IN, OUT> extends AbstractUdfStreamPreAggregateOperator<K, V, IN, OUT> {
	private static final long serialVersionUID = 1L;

	/**
	 * KeySelector is used to extract key for bundle map.
	 */
	private final KeySelector<IN, K> keySelector;

	public StreamPreAggregateOperator(PreAggregateFunction<K, V, IN, OUT> function,
									  PreAggregateTrigger<IN> preAggregateTrigger,
									  KeySelector<IN, K> keySelector) {
		super(function, preAggregateTrigger);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}
