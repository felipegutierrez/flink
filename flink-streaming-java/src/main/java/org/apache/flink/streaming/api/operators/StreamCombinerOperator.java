package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.CombineAdjustableFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.combiner.CombinerTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCombinerOperator<K, V, IN, OUT> extends AbstractUdfStreamCombinerOperator<K, V, IN, OUT> {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(StreamCombinerOperator.class);

	/**
	 * KeySelector is used to extract key for bundle map.
	 */
	private final KeySelector<IN, K> keySelector;

	public StreamCombinerOperator(CombineAdjustableFunction<K, V, IN, OUT> function,
								  CombinerTrigger<IN> combinerTrigger,
								  KeySelector<IN, K> keySelector) {
		super(function, combinerTrigger);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}
