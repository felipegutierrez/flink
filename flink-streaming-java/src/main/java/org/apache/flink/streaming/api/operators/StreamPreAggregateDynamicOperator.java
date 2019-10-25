package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.aggregation.PreAggregateDynamicTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamPreAggregateDynamicOperator<K, V, IN, OUT> extends AbstractUdfStreamPreAggregateDynamicOperator<K, V, IN, OUT> {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(StreamPreAggregateDynamicOperator.class);

	/**
	 * KeySelector is used to extract key for bundle map.
	 */
	private final KeySelector<IN, K> keySelector;

	public StreamPreAggregateDynamicOperator(PreAggregateFunction<K, V, IN, OUT> function,
											 PreAggregateDynamicTrigger<K, IN> bundleTrigger,
											 KeySelector<IN, K> keySelector) {
		super(function, bundleTrigger);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}
